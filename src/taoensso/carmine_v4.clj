(ns ^:no-doc taoensso.carmine-v4
  "Experimental modern Clojure Redis client prototype.
  Still private, not yet intended for public use!"
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [taoensso.encore  :as enc]
   [taoensso.carmine :as v3-core]
   [taoensso.carmine
    [connections :as v3-conns]
    [protocol    :as v3-protocol]
    [commands    :as v3-cmds]]

   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp.read   :as read]
   [taoensso.carmine-v4.resp.write  :as write]
   [taoensso.carmine-v4.resp        :as resp]
   ;;
   [taoensso.carmine-v4.utils       :as utils]
   [taoensso.carmine-v4.opts        :as opts]
   [taoensso.carmine-v4.conns       :as conns]
   [taoensso.carmine-v4.sentinel    :as sentinel]
   [taoensso.carmine-v4.cluster     :as cluster]))

(enc/assert-min-encore-version [3 112 0])

(comment (remove-ns 'taoensso.carmine-v4))

;;;; Config

(def ^:dynamic *issue-83-workaround?*
  "TODO Doc env config
  A bug in Carmine v2.6.0 to v2.6.1 (2014-04-01 to 2014-05-01)
  caused Nippy blobs to be marked incorrectly (with `ba-bin` instead
  of `ba-npy`), Ref. <https://github.com/ptaoussanis/carmine/issues/83>

  This should be kept true (the default) if there's a chance you might
  read any data written by Carmine < v2.6.1 (2014-05-01).

  Only relevant if `*auto-deserialize?` is true."
  (enc/get-env {:as :bool, :default true}
    :taoensso.carmine.issue-83-workaround))

(def ^:dynamic *auto-serialize?*
  "TODO Doc env config
  Should Carmine automatically serialize arguments sent to Redis
  that are non-native to Redis?

  Affects non-(string, keyword, simple long/double) types.

  Default is true. If false, an exception will be thrown when trying
  to send such arguments.

  See also `*auto-deserialize?`*."
  (enc/get-env {:as :bool, :default true}
    :taoensso.carmine.auto-serialize))

(def ^:dynamic *auto-deserialize?*
  "TODO Doc env config
  Should Carmine automatically deserialize Redis replies that
  contain data previously serialized by `*auto-serialize?*`?

  Affects non-(string, keyword, simple long/double) types.

  Default is true. If false, such replies will by default look like
  malformed strings.
  TODO: Mention utils, bindings.

  See also `*auto-serialize?`*."
  (enc/get-env {:as :bool, :default true}
    :taoensso.carmine.auto-deserialize))

(def ^:dynamic *keywordize-maps?*
  "Keywordize string keys in map-type Redis replies?"
  true)

(def ^:dynamic *freeze-opts*
  "TODO Docstring"
  nil)

(def default-pool-opts
  "TODO Docstring: describe `pool-opts`, env config.
  Ref. <https://commons.apache.org/proper/commons-pool/apidocs/org/apache/commons/pool2/impl/GenericKeyedObjectPool.html>,
       <https://commons.apache.org/proper/commons-pool/apidocs/org/apache/commons/pool2/impl/BaseGenericObjectPool.html>"
  (enc/nested-merge opts/default-pool-opts
    (enc/get-env {:as :edn} :taoensso.carmine.default-pool-opts)))

(def default-conn-manager-pooled_
  "TODO Docstring"
  (delay (conns/conn-manager-pooled {:pool-opts default-pool-opts})))

(comment @default-conn-manager-pooled_)

(def ^:dynamic *default-conn-opts*
  "TODO Docstring: describe `conn-opts`, env config."
  (opts/parse-conn-opts false
    (enc/nested-merge opts/default-conn-opts
      (enc/get-env {:as :edn} :taoensso.carmine.default-conn-opts))))

(def ^:dynamic *default-sentinel-opts*
  "TODO Docstring: describe `sentinel-opts`, env config."
  (opts/parse-sentinel-opts
    (enc/nested-merge opts/default-sentinel-opts
      (enc/get-env {:as :edn} :taoensso.carmine.default-sentinel-opts))))

(def ^:dynamic *conn-cbs*
  "Map of any additional callback fns, as in `conn-opts` or `sentinel-opts`.
  Useful for REPL/debugging/tests/etc.

  Possible keys:
    `:on-conn-close`
    `:on-conn-error`
    `:on-resolve-success`
    `:on-resolve-error`
    `:on-changed-master`
    `:on-changed-replicas`
    `:on-changed-sentinels`

  Values should be unary callback fns of a single data map."
  nil)

;;;; Aliases

(enc/defaliases
  enc/get-env

  ;;; Read opts
  com/skip-replies
  com/normal-replies
  com/as-bytes
  com/as-thawed
  com/natural-reads

  ;;; Reply parsing
  com/reply-error?
  com/unparsed
  com/parse
  com/parse-aggregates
  com/completing-rf
  ;;
  com/as-?long
  com/as-?double
  com/as-?kw
  ;;
  com/as-long
  com/as-double
  com/as-kw

  ;;; Write wrapping
  write/to-bytes
  write/to-frozen

  ;;; RESP3
  resp/redis-call
  resp/redis-call*
  resp/redis-calls
  resp/redis-calls*
  resp/local-echo
  resp/local-echos
  resp/local-echos*

  ;;; Connections
  conns/conn?
  conns/conn-ready?
  conns/conn-close!
  ;;
  sentinel/sentinel-spec
  sentinel/sentinel-spec?
  ;;
  conns/conn-manager?
  conns/conn-manager-unpooled
  conns/conn-manager-pooled
  {:alias conn-manager-init!           :src conns/mgr-init!}
  {:alias conn-manager-ready?          :src conns/mgr-ready?}
  {:alias conn-manager-close!          :src conns/mgr-close!}
  {:alias conn-manager-master-changed! :src conns/mgr-master-changed!}

  ;;; Cluster
  cluster/cluster-key)

;;;; Core API (main entry point to Carmine)

(defn with-car
  "TODO Docstring: `*default-conn-opts*`, `wcar`, etc.
  body-fn takes [conn]"
  ([conn-opts                                  body-fn] (with-car conn-opts nil body-fn))
  ([conn-opts {:keys [as-vec?] :as reply-opts} body-fn]
   (let [{:keys [natural-reads?]}  reply-opts] ; Undocumented
     (conns/with-conn
       (conns/get-conn conn-opts true true)
       (fn [conn in out]
         (resp/with-replies in out natural-reads? as-vec?
           (fn [] (body-fn conn))))))))

(comment
  (do
    (def mgr-unpooled (conns/conn-manager-unpooled {}))
    (def mgr-pooled   (conns/conn-manager-pooled
                        {:pool-opts
                         {:test-on-create? false
                          :test-on-borrow? false
                          :test-on-return? false}})))

  (enc/qb 1e3 ; [97.84 99.07 23.1], unpooled port limited
    (with-car {:mgr            nil} (fn [conn] (resp/ping) (resp/local-echo conn)))
    (with-car {:mgr #'mgr-unpooled} (fn [conn] (resp/ping) (resp/local-echo conn)))
    (with-car {:mgr   #'mgr-pooled} (fn [conn] (resp/ping) (resp/local-echo conn))))

  (->     unpooled deref)
  (-> (-> pooled   deref) :stats_ deref)

  ;; Benching
  (let [conn-opts {:mgr #'mgr-pooled :init nil}]
    (enc/qb 1e4 ; [16.49 219.44 691.71]
      (with-car conn-opts (fn [_]))
      (with-car conn-opts (fn [_]                  (resp/ping)))
      (with-car conn-opts (fn [_] (dotimes [_ 100] (resp/ping)))))))

(defmacro wcar
  "TODO Docstring: `*default-conn-opts*`, `with-car`, etc."
  {:arglists
   '([conn-opts                   & body]
     [conn-opts {:keys [as-vec?]} & body])}

  [conn-opts & body]
  (let [[reply-opts body] (resp/parse-body-reply-opts body)]
    `(with-car ~conn-opts ~reply-opts
       (fn [~'__wcar-conn] ~@body))))

(comment
  (wcar {} (resp/redis-call "PING"))
  (wcar {} (resp/redis-call "set" "k1" 3))
  (wcar {} (resp/redis-call "get" "k1"))

  (wcar {}                 (resp/ping))
  (wcar {} {:as-vec? true} (resp/ping))
  (wcar {}  :as-vec        (resp/ping))
  
  (enc/qb 1e4 ; [209.87 222.09]
    (v3-core/wcar {}                  (v3-core/ping))
    (wcar         {:mgr #'mgr-pooled} (resp/ping))))

(defmacro with-replies
  "TODO Docstring
  Expects to be called within the body of `wcar` or `with-car`."
  {:arglists '([& body] [{:keys [as-vec?]} & body])}
  [& body]
  (let [[reply-opts body] (resp/parse-body-reply-opts body)
        {:keys [natural-reads? as-vec?]} reply-opts]

    `(resp/with-replies ~natural-reads? ~as-vec?
       (fn [] ~@body))))

;;;; Push API ; TODO

(defmulti  push-handler (fn [state [data-type :as data-vec]] data-type))
(defmethod push-handler :default [state data-vec] #_(println data-vec) nil)

(enc/defonce push-agent_
  (delay (agent nil :error-mode :continue)))

(def ^:dynamic *push-fn*
  "TODO Docstring: this and push-handler, etc.
  ?(fn [data-vec]) => ?effects.
  If provided (non-nil), this fn should never throw."
  (fn [data-vec]
    (send-off @push-agent_
      (fn [state]
        (try
          (push-handler state data-vec)
          (catch Throwable t
            ;; TODO Try publish error message?
            ))))))

;;;; Scratch

;; TODO For command docstrings
;; As with all Carmine Redis command fns: expects to be called within a `wcar`
;; body, and returns nil. The server's reply to this command will be included
;; in the replies returned by the enclosing `wcar`.
