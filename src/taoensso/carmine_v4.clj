(ns ^:no-doc taoensso.carmine-v4
  "Experimental modern Clojure Redis client prototype.
  Still private, not yet intended for public use!"
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:refer-clojure :exclude [bytes])
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

(enc/assert-min-encore-version [3 145 0])

(comment (remove-ns 'taoensso.carmine-v4))

;;;; Aliases

(enc/defaliases
  enc/get-env

  ;;; Read opts
  com/skip-replies
  com/normal-replies
  com/natural-replies
  com/as-bytes
  com/thaw

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
  write/bytes
  write/freeze

  ;;; RESP3
  resp/rcall
  resp/rcall*
  resp/rcalls
  resp/rcalls*
  resp/local-echo
  resp/local-echos
  resp/local-echos*

  ;;; Connections
  #_conns/conn?
  #_conns/conn-ready?
  #_conns/conn-close!
  ;;
  sentinel/sentinel-spec
  sentinel/sentinel-spec?
  ;;
  conns/conn-manager?
  conns/conn-manager-unpooled
  conns/conn-manager-pooled
  {:alias conn-manager-ready? :src conns/mgr-ready?}
  {:alias conn-manager-clear! :src conns/mgr-clear!}
  {:alias conn-manager-close! :src conns/mgr-close!}

  ;;; Cluster
  cluster/cluster-key)

;;;; Config

(def default-conn-opts
  "TODO Docstring incl. env config."
  (let [from-env (enc/get-env {:as :edn} :taoensso.carmine.default-conn-opts)
        base
        {:server ["127.0.0.1" 6379]
         #_{:host "127.0.0.1" :port "6379"}
         #_{:master-name  "my-master"
            :sentinel-spec my-spec
            :sentinel-opts {}}

         :cbs         {:on-conn-close nil, :on-conn-error nil}
         :buffer-opts {:init-size-in 8192, :init-size-out 8192}
         :socket-opts {:ssl false, :connect-timeout-ms 400, :read-timeout-ms nil
                       :ready-timeout-ms 200}
         :init
         {;; :commands [["HELLO" 3 "AUTH" "default" "my-password" "SETNAME" "client-name"]
          ;;            ["auth" "default" "my-password"]]
          :resp3? true
          :auth {:username "default" :password nil}
          ;; :client-name "carmine"
          ;; :select-db   5
          }}]

    (enc/nested-merge base from-env)))

(def default-pool-opts
  "TODO Docstring incl. env config."
  (let [from-env (enc/get-env {:as :edn} :taoensso.carmine.default-pool-opts)
        base
        {:test-on-create?               true
         :test-while-idle?              true
         :test-on-borrow?               true
         :test-on-return?               false
         :num-tests-per-eviction-run    -1
         :min-evictable-idle-time-ms    60000
         :time-between-eviction-runs-ms 30000
         :max-total                     16
         :max-idle                      16}]

    (enc/nested-merge base from-env)))

(def default-sentinel-opts
  "TODO Docstring incl. env config."
  (let [from-env (enc/get-env {:as :edn} :taoensso.carmine.default-sentinel-opts)
        base
        {:cbs
         {:on-resolve-success   nil
          :on-resolve-error     nil
          :on-changed-master    nil
          :on-changed-replicas  nil
          :on-changed-sentinels nil}

         :update-sentinels?     true
         :update-replicas?      false
         :prefer-read-replica?  false

         :retry-delay-ms        250
         :resolve-timeout-ms    2000
         :clear-timeout-ms      10000

         :conn-opts
         {:cbs         {:on-conn-close nil, :on-conn-error nil}
          :buffer-opts {:init-size-in 512, :init-size-out 256}
          :socket-opts {:ssl false, :connect-timeout-ms 200, :read-timeout-ms 200
                        :ready-timeout-ms 200}}}]

    (enc/nested-merge base from-env)))

;;;;

(def ^:dynamic *auto-freeze?*
  "TODO Docstring incl. env config.
  Should Carmine automatically serialize arguments sent to Redis
  that are non-native to Redis?

  Affects non-(string, keyword, simple long/double) types.

  Default is true. If false, an exception will be thrown when trying
  to send such arguments.

  See also `*auto-freeze?`*."
  (enc/get-env {:as :bool, :default true}
    :taoensso.carmine.auto-freeze))

(def ^:dynamic *auto-thaw?*
  "TODO Docstring incl. env config.
  Should Carmine automatically deserialize Redis replies that
  contain data previously serialized by `*auto-thaw?*`?

  Affects non-(string, keyword, simple long/double) types.

  Default is true. If false, such replies will by default look like
  malformed strings.
  TODO: Mention utils, bindings.

  See also `*auto-thaw?`*."
  (enc/get-env {:as :bool, :default true}
    :taoensso.carmine.auto-thaw))

;; TODO Docstrings incl. env config.
(def ^:dynamic *raw-verbatim-strings?* false)
(def ^:dynamic *keywordize-maps?*       true)
(def ^:dynamic *freeze-opts*             nil)

(def ^:dynamic *issue-83-workaround?*
  "TODO Docstring incl. env config.
  Only relevant if `*auto-thaw?` is true.

  A bug in Carmine v2.6.0 to v2.6.1 (2014-04-01 to 2014-05-01) caused Nippy blobs
  to be marked incorrectly, Ref. <https://github.com/ptaoussanis/carmine/issues/83>

  When enabled, this workaround will cause Carmine to automatically try thaw any
  reply byte data that starts with a valid Nippy header.

  Enable iff you might read data written by Carmine < v2.6.1 (2014-05-01).
  Disabled by default."
  (enc/get-env {:as :bool, :default false}
    :taoensso.carmine.issue-83-workaround))

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

;;;; Core API (main entry point to Carmine)

(def ^:private default-conn-manager
  (delay (conns/conn-manager-pooled {:mgr-name :default})))

(comment (force default-conn-manager))

(defn with-car
  "TODO Docstring"
  ([conn-mgr                                  body-fn] (with-car conn-mgr nil body-fn))
  ([conn-mgr {:keys [as-vec?] :as reply-opts} body-fn]
   (let [{:keys [natural-replies?]} reply-opts] ; Undocumented
     (conns/mgr-borrow! (force (or conn-mgr default-conn-manager))
       (fn [conn in out]
         (resp/with-replies in out natural-replies? as-vec?
           (fn [] (body-fn conn))))))))

(defmacro wcar
  "TODO Docstring"
  {:arglists
   '([conn-mgr                   & body]
     [conn-mgr {:keys [as-vec?]} & body])}

  [conn-mgr & body]
  (let [[reply-opts body] (resp/parse-body-reply-opts body)]
    `(with-car ~conn-mgr ~reply-opts
       (fn [~'__wcar-conn] ~@body))))

(comment
  (let [mgr1 (conns/conn-manager-unpooled {})
        mgr2 (conns/conn-manager-pooled   {})
        mgr3 (conns/conn-manager-pooled
               {:pool-opts
                {:test-on-create? false
                 :test-on-borrow? false
                 :test-on-return? false}})]

    (try
      (enc/qb 1e3 ; [22.33 97.37 38.87 19.86]
        (v3-core/wcar {}       (v3-core/ping))
        (with-car mgr1 (fn [conn] (resp/ping)))
        (with-car mgr2 (fn [conn] (resp/ping)))
        (with-car mgr3 (fn [conn] (resp/ping))))

      (finally
        (doseq [mgr [mgr1 mgr2 mgr3]]
          (conns/mgr-close! mgr 5000 nil)))))

  [(wcar nil (resp/rcall "PING"))
   (wcar nil (resp/rcall "set" "k1" 3))
   (wcar nil (resp/rcall "get" "k1"))

   (wcar nil                 (resp/ping))
   (wcar nil {:as-vec? true} (resp/ping))
   (wcar nil  :as-vec        (resp/ping))])

(defmacro with-replies
  "TODO Docstring
  Expects to be called within the body of `wcar` or `with-car`."
  {:arglists '([& body] [{:keys [as-vec?]} & body])}
  [& body]
  (let [[reply-opts body] (resp/parse-body-reply-opts body)
        {:keys [natural-replies? as-vec?]} reply-opts]
    `(resp/with-replies ~natural-replies? ~as-vec?
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
