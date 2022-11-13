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

   [taoensso.carmine-v4.opts     :as opts]
   [taoensso.carmine-v4.conns    :as conns]
   [taoensso.carmine-v4.sentinel :as sentinel]))

(enc/assert-min-encore-version [3 39 0])

(defn run-all-carmine-tests []
  (test/run-all-tests #"taoensso\.carmine-v4.*"))

(comment
  (remove-ns      'taoensso.carmine-v4)
  (test/run-tests 'taoensso.carmine-v4)
  (run-all-carmine-tests))

;;;; TODO

;; - mgr-master-changed! method

;; - finish `PooledConnManager/mgr-borrow!` impln
;; - retire `try-borrow-conn!`? How+where to wrap pool errors?
;;
;; - No issue with caching vs opts with metadata?
;;
;; - Think through, confirm: :mgr support should just work correctly
;;   within sentinel-opts/conn-opts, right?
;;
;; - Document and alias `pooled-conn-manager` + provide default?
;;
;; - Integrate `conns/get-conn` into `with-carmine`, etc.
;; - Low-level API combo: `get-conn` + `with-conn` + `resp/with-replies`, etc.

;;; flow
;; 1. User creates pool: (pooled-conn-manager <mgr-opts>)
;; 2. `wcar` calls: (with-conn <conn-opts> f), with :mgr in opts
;; 3. `with-conn` calls: (mgr-borrow! <mgr> <conn-opts-minus-mgr>)
;;     - or maybe add :skip-mgr? true
;; 4. `mgr-borrow!` produces <kop-key>, calls: (.borrowObject <kop-key>)
;; 5. `borrowObject` calls: (makeObject <kop-key>)
;; 6. `makeObject` calls: (new-conn (kop->opts <kop-key>))

#_
(defn- with-managed-conn
  "Calls (f <borrowed-Conn> <in> <out>) and returns/removes Conn after use.
  Throws if a Conn cannot be borrowed."
  [conn-mgr_ conn-opts f]
  (let [conn-mgr   (force conn-mgr_)
        conn (mgr-borrow! conn-mgr conn-opts)]
    (try
      (let [result (f conn (.-in conn) (.-out conn))]
        (mgr-return! conn-mgr conn)
        result)

      (catch Throwable t
        (mgr-remove! conn-mgr conn {:via 'with-managed-conn :cause t})
        (throw t)))))
#_
(defn- init-managed-conn [^Conn conn conn-opts]
  (when-let [so (not-empty (get conn-opts :socket-opts))]
    (when (contains? so :connect-timeout-ms)
      (.setSoTimeout ^Socket (.-socket conn)
        (int (or (get so :connect-timeout-ms) 0))))
    (when (contains? so :read-timeout-ms)
      (.setSoTimeout ^Socket (.-socket conn)
        (int (or (get so :read-timeout-ms) 0))))))

;; - Connections
;;   - Complete 1st draft, incl. managers
;;   - Any special cbs for `PooledConnManager`? Likely skip.
;;   - Decide on ^:public conns API. Plausible to extend IConnManager?
;;   - Re-check Sentinel client docs

;; - Consider pseudo Redis commands:
;;   - set-socket-timeout!
;;   - close-socket!

;; - Core: new Pub/Sub API
;; - Sentinel: integrate with Pub/Sub - mgr-master-change!
;; - Common & core util to parse-?marked-ba -> [<kind> <payload>]

;; - Test `resp/basic-ping!`
;; - High-level tests -> `taoensso.carmine-v4.tests.main`
;;   - Test conn & mgrs
;;     - Ability to interrupt long-blocking reqs (grep "v3 conn closing" in this ns)
;;     - Hard & soft shutdown

;; - Investigate Cluster

;; - Polish
;;   - Check ns layout + hierarchy, incl. conns, replies, types, tests
;;   - `defprotocol` docstrings
;;   - Check all errors: eids, messages, data, cb-ids
;;   - Check all dynamic bindings and sys-vals, ensure accessible
;;   - Document `*default-conn-opts*`,     incl. cbs, Sentinel :server
;;   - Document `*default-sentinel-opts*`, incl. cbs
;;   - Grep for TODOs

;; - Refactor commands, add modules support
;; - Refactor pub/sub, etc. (note RESP2 vs RESP3 differences)
;; - Refactor helpers API, etc.
;; - Consider later refactoring mq?

;; - Final Jedis IO benching (grep for `.read`), and/or remove Jedis code?
;;   - `jedis.RedisInputStream`: readLineBytes, readIntCrLf, readLongCrLf
;;   - `jedis.RedisOutputStream`: writeCrLf, writeIntCrLf

;; - Plan for ->v4 upgrade with back compatibility? ^{:deprecated <str>}

;; - v4 wiki with changes, migration, new features, examples, etc.
;;   - Mention `redis-call`, esp. re: modules and new API stuff
;; - First alpha release

;; - Could add `to-streaming-freeze` that uses the RESP3 API streaming bulk
;;   type to freeze objects directly to output stream (i.e. without intermediary
;;   ba)? Probably most useful for large objects, but complex, involves tradeoffs,
;;   and how often would this be useful?

;;;; CHANGELOG
;; - [new] Full RESP3 support, incl. streaming, etc.
;;   - Enabled by default, requires Redis >= v6 (2020-04-30)
;; - [new] *auto-serialize?*, *auto-deserialize?*
;; - [new] Greatly improved `skip-replies` performance
;; - [mod] Simplified parsers API
;; - [new] Aggregate  parsers, with xform support
;; - [new] New, improved documentation - incl. docstrings & wiki
;; - [new] Improved error messages, with better debug data
;; - [new] Improved instrumentation for conns and conn management
;; - [new] Greatly improved flexibility re: connections and connection management
;; - [new] Greatly improved usability re: connections, incl. opts validation
;;         and clear error messages for problems
;; - [new] Pool efficiency improvements, incl. better sub-pool keying
;; - [new] Greatly improved instrumentation options for conns and pools

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
  TODO: Mention utils, bindings

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

(def ^:dynamic *default-conn-opts*
  "TODO Docstring, describe conn-opts, edn-config"
  (let [{:keys [config]}
        (enc/load-edn-config
          {:prop "taoensso.carmine.default-conn-opts.edn"
           :res  "taoensso.carmine.default-conn-opts.edn"
           :default opts/default-conn-opts})]

    (conns/valid-conn-opts false config)))

(def ^:dynamic *default-sentinel-opts*
  "TODO Docstring, describe sentinel-opts, edn-config"
  (let [{:keys [config]}
        (enc/load-edn-config
          {:prop "taoensso.carmine.default-sentinel-opts.edn"
           :res  "taoensso.carmine.default-sentinel-opts.edn"
           :default opts/default-sentinel-opts})]

    (sentinel/valid-sentinel-opts config)))

(def default-pool-opts
  "TODO Docstring, describe edn-config

  Ref. https://commons.apache.org/proper/commons-pool/apidocs/org/apache/commons/pool2/impl/GenericKeyedObjectPool.html,
       https://commons.apache.org/proper/commons-pool/apidocs/org/apache/commons/pool2/impl/BaseGenericObjectPool.html"

  (let [{:keys [config]}
        (enc/load-edn-config
          {:prop "taoensso.carmine.default-pool-opts.edn"
           :res  "taoensso.carmine.default-pool-opts.edn"
           :default opts/default-pool-opts})]
    config))

;;;; Aliases

(do
  (enc/defalias com/reply-error?)

  (do ; Read opts
    (enc/defalias com/skip-replies)
    (enc/defalias com/normal-replies)
    (enc/defalias com/as-bytes)
    (enc/defalias com/as-thawed)
    (enc/defalias com/natural-reads))

  (do ; Reply parsing
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
    (enc/defalias write/to-frozen) ; TODO Docstring
    )

  (do
    (enc/defalias resp/redis-call)
    (enc/defalias resp/redis-call*)
    (enc/defalias resp/local-echo))

  (do ; Connections
    
    (enc/defalias conns/conn?)
    (enc/defalias conns/conn-manager?)

    (enc/defalias conns/close-conn)
    (enc/defalias conns/unpooled-conn-manager)

    (enc/defalias sentinel/sentinel-spec)
    (enc/defalias sentinel/sentinel-spec?)

    (enc/defalias mgr-init!)
    (enc/defalias mgr-ready?)
    (enc/defalias mgr-close!)

    ;; TODO
    ))

;;;; Push API ; TODO

(defmulti  push-handler (fn [state [data-type :as data-vec]] data-type))
(defmethod push-handler :default [state data-vec] #_(println data-vec) nil)

(enc/defonce push-agent_
  (delay (agent nil :error-mode :continue)))

(def ^:dynamic *push-fn* ; TODO move to core
  "?(fn [data-vec]) => ?effects.
  If provided (non-nil), this fn should never throw."
  ;; TODO Proper docstring for this & push-handler, etc.
  (fn [data-vec]
    (send-off @push-agent_
      (fn [state]
        (try
          (push-handler state data-vec)
          (catch Throwable t
            ;; TODO Try publish error message?
            ))))))


;;;; Scratch

;; TODO For commands
;; As with all Carmine Redis command fns: expects to be called within a `wcar`
;; body, and returns nil. The server's reply to this command will be included
;; in the replies returned by the enclosing `wcar`.

(defn nconn
  ([    ] (nconn {}))
  ([opts]
   (let [{:keys [host port]
          :or   {host "127.0.0.1"
                 port 6379}}
         opts]

     (v3-conns/make-new-connection
       {:host host :port port}))))

(comment (keys (nconn))) ; (:socket :spec :in :out)


(comment ; TODO Testing v3 conn closing
  ;; TODO Make a test
  (def c (nconn))

  ;; Push
  (let [{:keys [in out]} c]
    (resp/with-replies in out false false
      (fn [] (resp/redis-call "lpush" "l1" "x"))))

  ;; Pop
  (future
    (let [{:keys [in out]} c
          reply
          (try
            (resp/with-replies in out false false
              (fn []
                (resp/redis-call "blpop" "l1" 3)))
            (catch Throwable t t))]
      (println "RESPONSE: " reply)))

  (let [{:keys [in out]} c]
    (resp/with-replies in out false false
      (fn []
        (resp/redis-call "blpop" "l1" "3"))))

  (v3-conns/close-conn c))



(defn with-carmine
  "Low-level util, prefer `wcar` instead."

  ;; todo mention *default-conn-opts*
  ;; todo alias as with-car

  ;; TODO :sentinel {:spec <spec> :master <master-name> ... <opts>}
  [opts body-fn]
  (let [{:keys [conn natural-reads? as-vec?]} opts
        {:keys [in out]} (or conn (nconn opts))]

    (resp/with-replies in out natural-reads? as-vec?
      body-fn)))

(defmacro wcar
  "TODO Docstring"
  ;; todo mention *default-conn-opts*
  [opts & body]
  `(with-carmine ~opts
     (fn [] ~@body)))

(comment :see-tests)

(defmacro with-replies
  "TODO Docstring
  Expects to be called within the body of a `wcar`."
  [& body]
  (let [[opts body] (let [[b1 & bn] body] (if (map? b1) [b1 bn] [nil body]))
        {:keys [natural-reads? as-vec?]} opts]

    `(resp/with-replies ~natural-reads? ~as-vec?
       (fn [] ~@body))))

(comment :see-tests)

(comment
  (wcar {} (resp/redis-call "set" "k1" 3))
  (wcar {} (resp/redis-call "get" "k1"))
  (wcar {}              (resp/ping))
  (wcar {:as-vec? true} (resp/ping))

  (let [{:keys [in out]} (nconn)]
    (enc/qb 1e4 (resp/basic-ping! in out))) ; 210.2

  ;; 234.77
  (let [opts {:conn (nconn)}]
    (enc/qb 1e4 (wcar opts (resp/redis-call "ping")))))

;;;;

(comment
  (v3-protocol/with-context (nconn)
    (v3-protocol/with-replies
      (v3-cmds/enqueue-request 1 ["SET" "KX" "VY"])
      (v3-cmds/enqueue-request 1 ["GET" "KX"])))

  (let [c (nconn)] (.read (:in c))) ; Inherently blocking
  (let [c (nconn)] (v3-conns/close-conn c) (.read (:in c))) ; Closed
  )