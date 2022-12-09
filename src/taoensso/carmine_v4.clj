(ns taoensso.carmine-v4
  "Experimental, baggage-free modern Redis client prototype."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.test     :as test :refer [deftest testing is]]
   [taoensso.encore  :as enc  :refer [throws?]]
   [taoensso.carmine :as v3-core]
   [taoensso.carmine
    [connections :as v3-conns]
    [protocol    :as v3-protocol]
    [commands    :as v3-cmds]]

   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp.read   :as read]
   [taoensso.carmine-v4.resp.write  :as write]
   [taoensso.carmine-v4.resp        :as resp]

   [taoensso.carmine-v4.utils    :as utils]
   [taoensso.carmine-v4.opts     :as opts]
   [taoensso.carmine-v4.conns    :as conns]
   [taoensso.carmine-v4.sentinel :as sentinel]
   [taoensso.carmine-v4.cluster  :as cluster]))

(enc/assert-min-encore-version [3 39 0])

(defn run-all-carmine-tests []
  (test/run-all-tests #"taoensso\.carmine-v4.*"))

(comment
  (remove-ns      'taoensso.carmine-v4)
  (test/run-tests 'taoensso.carmine-v4)
  (run-all-carmine-tests))

;;;; TODO

;; x Investigate Cluster
;; - Pause v4 work for now?
;; - Implement Cluster?

;; - Common & core util to parse-?marked-ba -> [<kind> <payload>]
;; - Core: new Pub/Sub API
;;   - Pub/Sub + Sentinel integration
;;     - psubscribe* to Sentinel server
;;       - check for `switch-master` channel name
;;         - "switch-master" <master-name> <old-ip> <old-port> <new-ip> <new-port>

;; - Polish
;;   - Check all errors: eids, messages, data, cbids
;;   - Check all dynamic bindings and sys-vals, ensure accessible
;;   - Document `*default-conn-opts*`,     incl. cbs
;;   - Document `*default-sentinel-opts*`, incl. cbs
;;   - Complete (esp. high-level / integration) tests
;;   - Grep for TODOs

;; - Refactor commands
;;   - Add modules support
;;   - Also support custom (e.g. newer) commands.json or edn
;; - Refactor pub/sub, etc. (note RESP2 vs RESP3 differences)
;; - Refactor helpers API, etc.
;; - Modern MQ?
;; - Modern Tundra?

;; - Plan for ->v4 upgrade with back compatibility? ^{:deprecated <str>}
;; - v4 wiki with changes, migration, new features, examples, etc.
;;   - Mention `redis-call`, esp. re: modules and new API stuff
;;   - Incl. new docs for mq.
;; - First alpha release

;;;; CHANGELOG
;; - [new] Full RESP3 support, incl. streaming, etc.
;;   - Enabled by default, requires Redis >= v6 (2020-04-30).
;; - [new] Full Redis Sentinel support - incl. auto failover and read replicas.
;; - [mod] Hugely improved connections API, incl. improved:
;;   - Flexibility
;;   - Docs
;;   - Usability (e.g. opts validation, hard shutdowns,
;;     closing managed conns, etc.).
;;   - Transparency (deref stats, cbs, timings for profiling, etc.).
;;     - Derefs: Conns, ConnManagers, SentinelSpecs.
;;   - Protocols for extension by advanced users.
;;   - Full integration with Sentinel, incl.:
;;     - Auto invalidation of pool conns on master changes.
;;     - Auto verification of addresses on pool borrows.
;;
;; - [new] Common conn utils are now aliased in core Carmine ns for convenience.
;; - [new] Improved pool efficiency, incl. smarter sub-pool keying.
;; - [mod] Improved parsing API, incl.:
;;   - General simplifications.
;;   - Aggregate parsers, with xform support.
;;
;; - [new] *auto-serialize?*, *auto-deserialize?*
;; - [new] Greatly improved `skip-replies` performance
;; - [mod] Simplified parsers API
;;
;; - [new] Improvements to docs, error messages, debug data, etc.
;; - [new] New Wiki with further documentation and examples.

;;;; Config

(def issue-83-workaround?
  "A bug in Carmine v2.6.0 to v2.6.1 (2014-04-01 to 2014-05-01)
  caused Nippy blobs to be marked incorrectly (with `ba-bin` instead
  of `ba-npy`), Ref. https://github.com/ptaoussanis/carmine/issues/83

  This should be kept true (the default) if there's a chance you might
  read any data written by Carmine < v2.6.1.

  Only relevant if `*auto-deserialize?` is true."

  (enc/get-sys-bool true
    "taoensso.carmine.issue-83-workaround"
    "TAOENSSO_CARMINE_ISSUE_83_WORKAROUND"))

(def ^:dynamic *auto-serialize?*
  "Should Carmine automatically serialize arguments sent to Redis
  that are non-native to Redis?

  Affects non-(string, keyword, simple long/double) types.

  If falsey, an exception will be thrown when trying to send
  such arguments.

  Default: true.
  Compile-time default can be overridden with:
    - `taoensso.carmine.auto-serialize` JVM property (read as bool)
    - `TAOENSSO_CARMINE_AUTO_SERIALIZE` env var      (read as bool)

  See also `*auto-deserialize?`*."

  (enc/get-sys-bool true
    "taoensso.carmine.auto-serialize"
    "TAOENSSO_CARMINE_AUTO_SERIALIZE"))

(def ^:dynamic *auto-deserialize?*
  "Should Carmine automatically deserialize Redis replies that
  contain data previously serialized by `*auto-serialize?*`?

  Affects non-(string, keyword, simple long/double) types.

  If false, such replies will by default look like malformed strings.
  TODO: Mention utils, bindings.

  Default: true.
  Compile-time default can be overridden with:
    - `taoensso.carmine.auto-deserialize` JVM property (read as bool)
    - `TAOENSSO_CARMINE_AUTO_DESERIALIZE` env var      (read as bool)

  See also `*auto-serialize?`*."

  (enc/get-sys-bool true
    "taoensso.carmine.auto-deserialize"
    "TAOENSSO_CARMINE_AUTO_DESERIALIZE"))

(def ^:dynamic *keywordize-maps?*
  "Keywordize string keys in map-type Redis replies?"
  true)

(def ^:dynamic *freeze-opts*
  "TODO Docstring"
  nil)

(def default-pool-opts
  "TODO Docstring: describe `pool-opts`, edn-config.

  Ref. https://commons.apache.org/proper/commons-pool/apidocs/org/apache/commons/pool2/impl/GenericKeyedObjectPool.html,
       https://commons.apache.org/proper/commons-pool/apidocs/org/apache/commons/pool2/impl/BaseGenericObjectPool.html"

  (let [{:keys [config]}
        (enc/load-edn-config
          {:prop "taoensso.carmine.default-pool-opts.edn"
           :res  "taoensso.carmine.default-pool-opts.edn"
           :default opts/default-pool-opts})]
    config))

(def default-conn-manager-pooled_
  "TODO Docstring"
  (delay (conns/conn-manager-pooled {:pool-opts default-pool-opts})))

(comment @default-conn-manager-pooled_)

(def ^:dynamic *default-conn-opts*
  "TODO Docstring: describe `conn-opts`, edn-config."
  (let [{:keys [config]}
        (enc/load-edn-config
          {:prop "taoensso.carmine.default-conn-opts.edn"
           :res  "taoensso.carmine.default-conn-opts.edn"
           :default opts/default-conn-opts})]

    (opts/parse-conn-opts false config)))

(def ^:dynamic *default-sentinel-opts*
  "TODO Docstring: describe `sentinel-opts`, edn-config."
  (let [{:keys [config]}
        (enc/load-edn-config
          {:prop "taoensso.carmine.default-sentinel-opts.edn"
           :res  "taoensso.carmine.default-sentinel-opts.edn"
           :default opts/default-sentinel-opts})]

    (opts/parse-sentinel-opts config)))

(def ^:dynamic *conn-cbs*
  "Map of any additional callback fns, as in `conn-opts` or `sentinel-opts`.
  Useful for REPL/debugging/tests/etc.

  Possible keys:
    :on-conn-close
    :on-conn-error
    :on-resolve-success
    :on-resolve-error
    :on-changed-master
    :on-changed-replicas
    :on-changed-sentinels

  Values should be unary callback fns of a single data map."

  nil)

;;;; Aliases

(do
  (do ; Encore
    (enc/defalias enc/load-edn-config))

  (do ; Read opts
    (enc/defalias com/skip-replies)
    (enc/defalias com/normal-replies)
    (enc/defalias com/as-bytes)
    (enc/defalias com/as-thawed)
    (enc/defalias com/natural-reads))

  (do ; Reply parsing
    (enc/defalias com/reply-error?)
    (enc/defalias com/unparsed)
    (enc/defalias com/parse)
    (enc/defalias com/parse-aggregates)
    (enc/defalias com/completing-rf)

    (enc/defalias com/as-?long)
    (enc/defalias com/as-?double)
    (enc/defalias com/as-?kw)

    (enc/defalias com/as-long)
    (enc/defalias com/as-double)
    (enc/defalias com/as-kw))

  (do ; Write wrapping
    (enc/defalias write/to-bytes)
    (enc/defalias write/to-frozen))

  (do ; RESP3
    (enc/defalias resp/redis-call)
    (enc/defalias resp/redis-call*)
    (enc/defalias resp/redis-calls)
    (enc/defalias resp/redis-calls*)
    (enc/defalias resp/local-echo)
    (enc/defalias resp/local-echos)
    (enc/defalias resp/local-echos*))

  (do ; Connections
    (enc/defalias conns/conn?)
    (enc/defalias conns/conn-ready?)
    (enc/defalias conns/conn-close!)

    (enc/defalias sentinel/sentinel-spec)
    (enc/defalias sentinel/sentinel-spec?)

    (enc/defalias conns/conn-manager?)
    (enc/defalias conns/conn-manager-unpooled)
    (enc/defalias conns/conn-manager-pooled)
    (enc/defalias       conn-manager-init!           conns/mgr-init!)
    (enc/defalias       conn-manager-ready?          conns/mgr-ready?)
    (enc/defalias       conn-manager-close!          conns/mgr-close!)
    (enc/defalias       conn-manager-master-changed! conns/mgr-master-changed!))

  (do ; Cluster
    (enc/defalias cluster/cluster-key)))

;;;; Core API (main entry point to Carmine)

(defn with-car
  "TODO Docstring: `*default-conn-opts*`, `wcar`, etc.
  body-fn takes [conn]"
  ([conn-opts                                  body-fn] (with-car conn-opts nil body-fn))
  ([conn-opts {:keys [as-vec?] :as reply-opts} body-fn]
   (let [{:keys [natural-reads?]}  reply-opts] ; Undocumented
     (conns/with-conn
       (conns/get-conn conn-opts :parse-opts :use-mgr)
       (fn [conn in out]
         (resp/with-replies in out natural-reads? as-vec?
           (fn [] (body-fn conn))))))))

(comment :see-tests)
(comment
  (do
    (def unpooled (conns/conn-manager-unpooled {}))
    (def   pooled (conns/conn-manager-pooled
                    {:pool-opts
                     {:test-on-create? false
                      :test-on-borrow? false}})))

  (with-car {:mgr        nil} (fn [conn] (resp/ping) (resp/local-echo conn)))
  (with-car {:mgr #'unpooled} (fn [conn] (resp/ping) (resp/local-echo conn)))
  (with-car {:mgr   #'pooled} (fn [conn] (resp/ping) (resp/local-echo conn)))

  (->     unpooled deref)
  (-> (-> pooled   deref) :stats_ deref)

  ;; Benching
  (let [conn-opts {:mgr #'pooled :init nil}]
    (enc/qb 1e4 ; [47.49 321.34 1075.16]
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

(comment :see-tests)
(comment
  (wcar {} (resp/redis-call "PING"))
  (wcar {} (resp/redis-call "set" "k1" 3))
  (wcar {} (resp/redis-call "get" "k1"))

  (wcar {}                 (resp/ping))
  (wcar {} {:as-vec? true} (resp/ping))
  (wcar {}  :as-vec        (resp/ping))

  (enc/qb 1e4 ; 841.29 (with pool testing)
    (wcar {} (resp/ping))))

(defmacro with-replies
  "TODO Docstring
  Expects to be called within the body of `wcar` or `with-car`."
  {:arglists '([& body] [{:keys [as-vec?]} & body])}
  [& body]
  (let [[reply-opts body] (resp/parse-body-reply-opts body)
        {:keys [natural-reads? as-vec?]} reply-opts]

    `(resp/with-replies ~natural-reads? ~as-vec?
       (fn [] ~@body))))

(comment :see-tests)

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
