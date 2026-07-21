(ns taoensso.carmine-v4.tests.main
  "Carmine v4 public API, compatibility, and generated-command tests."
  (:require
   [clojure.test        :as test :refer [deftest testing is]]
   [taoensso.encore     :as enc]
   [taoensso.truss      :as truss :refer [throws?]]
   [taoensso.carmine    :as v3-core]
   [taoensso.carmine-v4          :as car  :refer [wcar with-replies]]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp.write  :as write]
   [taoensso.carmine-v4.resp     :as resp]
   [taoensso.carmine-v4.utils    :as utils]
   [taoensso.carmine-v4.opts     :as opts]
   [taoensso.carmine-v4.conns    :as conns]
   [taoensso.carmine-v4.sentinel :as sentinel]
   [taoensso.carmine-v4.cluster  :as cluster]
   [taoensso.carmine-v4.tests.test-support :as support]))

(def tk  "Test key" support/test-key)
(def tc  "Unparsed test conn-opts" support/test-conn-opts)
(def tc+ "Parsed test conn-opts" support/parsed-test-conn-opts)
(def mgr_ support/manager_)

(support/use-clean-redis-fixture!)

(defn- redis-major-version [] (support/redis-major-version))

;;;; Public API

(deftest _public-api-contract
  (let [publics       (ns-publics 'taoensso.carmine-v4)
        generated     (into {} (filter (comp :redis-api meta val)) publics)
        no-doc        (into #{} (keep (fn [[sym v]] (when (:no-doc (meta v)) sym))) publics)
        curated       (apply dissoc publics (concat (keys generated) no-doc))
        command-spec  (enc/read-edn (enc/slurp-resource "carmine-commands.edn"))
        command-metas (into {} (map (fn [[_ v]] [(:redis-command (meta v)) (meta v)])) generated)
        expected-curated
        '#{*auto-freeze?* *auto-thaw?* *conn-cbs* *freeze-opts*
           *issue-83-workaround?* *keywordize-maps?*
           *raw-verbatim-strings?*
           as-?double as-?kw as-?long as-bytes as-double as-kw as-long bytes
           cluster-cached-topology cluster-key cluster-refresh-topology!
           cluster-slot cluster-spec cluster-spec? with-cluster-target
           conn-manager-clear! conn-manager-close! conn-manager-clustered
           conn-manager conn-manager-open? conn-manager-unpooled
           conn-manager? conn-manager-stats
           compare-and-delete compare-and-hdel compare-and-hset compare-and-set
           default-cluster-opts default-cluster-spec-opts
           default-cluster-pool-opts default-conn-opts
           default-pool-opts default-sentinel-opts default-sentinel-spec-opts
           freeze hswap local-echo
           eval* eval-ro* evalsha* lua lua-ro prepare-lua script-hash
           natural-replies normal-replies parse parse-aggregates
           pubsub-await-synced! pubsub-close! pubsub-listener pubsub-listener?
           pubsub-ping! pubsub-stats
           pubsub-subscribe! pubsub-unsubscribe!
           rcmd rcmd*
           reply-attributes reply-content reply-error?
           scan-keys scan-reduce scan-reduce-kv sentinel-spec sentinel-spec?
           skip-replies swap thaw transact! unparsed wcar with-car with-replies}]

    [(is (= (count generated) (count command-spec))
       "Every bundled Redis command has one generated public var")
     (is (= no-doc
           '#{dummy-scan-fn scan-reduce-elements
              local-echos local-echos* rcmds rcmds*}))
     (is (= (set (keys curated)) expected-curated)
       "The intentional non-generated public surface changes explicitly")
     (is (every? (comp string? :doc meta val) curated))
     (is (every?
           (fn [[_ v]]
             (let [{:keys [arglists doc redis-command]} (meta v)]
               (and (seq arglists) (not-empty doc) (not-empty redis-command))))
           generated))
     (is (every? #(false? (get-in command-metas [% :supports-cluster?]))
           ["SELECT" "MOVE" "SWAPDB" "MULTI" "EXEC" "DISCARD" "WATCH" "UNWATCH"
            "PSYNC" "REPLCONF" "SYNC"]))
     (is (true? (get-in command-metas ["PING" :supports-cluster?])))
     (is (= (get-in command-metas ["PING" :redis-cluster-routing]) :single-node))
     (is (= (get-in command-metas ["MIGRATE" :redis-cluster-route-kind]) :migrate))
     (is (every? #(= (get-in command-metas [% :redis-cluster-routing]) :partial)
           ["SORT" "SORT_RO"]))]))


;;;; Compatibility and native commands

(deftest _v3-v4-storage-compatibility
  (let [v3-key (tk :v3-written)
        v4-key (tk :v4-written)
        v3-val {:writer :v3, :payload [1 2 3]}
        v4-val {:writer :v4, :payload #{:a :b}}]
    [(is (= (v3-core/wcar {} (v3-core/set v3-key v3-val)) "OK"))
     (is (= (wcar mgr_ (car/get v3-key)) v3-val)
       "v4 reads Carmine v3 Nippy-marked values")
     (is (= (wcar mgr_ (car/set v4-key v4-val)) "OK"))
     (is (= (v3-core/wcar {} (v3-core/get v4-key)) v4-val)
       "v3 reads Carmine v4 Nippy-marked values")]))

(deftest _live-redis-protocol-compatibility
  (let [actual-major   (redis-major-version)
        expected-major (some-> (System/getenv "CARMINE_TEST_REDIS_MAJOR") Long/parseLong)]
    [(is (pos-int? actual-major))
     (when expected-major
       (is (= actual-major expected-major)
         "CI is exercising the requested Redis major version"))])

  (doseq [resp3? (support/supported-resp3-options)]
    (let [key     (tk (str "protocol:" resp3? ":" (java.util.UUID/randomUUID)))
          missing (str key ":missing")
          value   {:protocol (if resp3? 3 2), :payload [1 2 3]}]
      (with-open [mgr
                  (car/conn-manager-unpooled
                    {:conn-opts {:init {:resp3? resp3?}}})]
        (try
          [(is (= (wcar mgr (car/ping)) "PONG"))
           (is (= (wcar mgr (car/set key value)) "OK"))
           (is (= (wcar mgr (car/mget missing key)) [nil value]))
           (let [replies
                 (wcar mgr {:as-vec? true, :error-mode :return}
                   (car/get key)
                   (car/incr key)
                   (car/ping))]
             [(is (= (first replies) value))
              (is (car/reply-error? {:eid :carmine.read/redis-error-reply} (second replies)))
              (is (= (nth replies 2) "PONG"))])]
          (finally
            (wcar mgr (car/del key))))))))

(deftest _native-command-api
  (let [k (tk :native-command)]
    [(is (= (wcar mgr_ :as-vec (car/set k "value") (car/get k))
           ["OK" "value"]))
     (is (= (:redis-command (meta #'car/get)) "GET"))
     (is (true? (:supports-cluster? (meta #'car/get))))
     (is (true? (:supports-cluster? (meta #'car/ping))))
     (is (false? (:supports-cluster? (meta #'car/multi))))
     (is (nil? (:redis-key-specs (meta #'car/migrate))))
     (is (= (:redis-cluster-routing (meta #'car/migrate)) :exact))
     (is (= (:redis-cluster-routing (meta #'car/sort)) :partial))
     (is (->> (car/get k)
           (throws? :ex-info {:eid :carmine/no-context})))])

  (let [pending-reqs (java.util.LinkedList.)
        ctx (taoensso.carmine_v4.resp.Ctx.
              false false pending-reqs (java.util.LinkedList.) nil nil nil nil)]
    (binding [resp/*ctx* ctx]
      (car/config-set "timeout" 10))
    (let [^taoensso.carmine_v4.resp.Req req (.getFirst pending-reqs)]
      [(is (= (.-args req) ["CONFIG" "SET" "timeout" 10])
         "Generated requests retain their original logical arguments")
       (is (= (.-n-prefix-args req) 2))
       (is (= (enc/utf8-ba->str (.-encoded-prefix req))
             "$6\r\nCONFIG\r\n$3\r\nSET\r\n"))]))

  (let [find-key
        (fn [pred prefix]
          (some
            (fn [idx]
              (let [key (str prefix idx)
                    slot (cluster/cluster-slot (cluster/cluster-key key))]
                (when (pred slot) key)))
            (range 100000)))
        key-a (find-key #(< % 8192) "native-a:")
        key-b (find-key #(>= % 8192) "native-b:")
        route (fn [v] (:redis-key-specs (meta v)))
        route-info
        (fn [v]
          (let [m (meta v)]
            {:kind (:redis-cluster-route-kind m)
             :key-specs (:redis-key-specs m)}))
        migrate-a "{native-migrate}:a"
        migrate-b "{native-migrate}:b"]
    [(is (= (cluster/command-slot ["GET" key-a] (route #'car/get))
           (cluster/cluster-slot (cluster/cluster-key key-a))))
     (is (= (cluster/command-slot ["GET" 1N] (route #'car/get))
            (cluster/command-slot
              (write/prepare-command ["GET" 1N]) (route #'car/get)))
       "Nippy-frozen keys hash their exact prepared wire representation")
     (is (->> (cluster/command-slot ["MGET" key-a key-b] (route #'car/mget))
           (throws? :ex-info {:eid :carmine.cluster/cross-slot-keys})))
     (is (->> (cluster/command-slot ["EVAL" "return 1" 2 key-a key-b]
                  (route #'car/eval))
           (throws? :ex-info {:eid :carmine.cluster/cross-slot-keys})))
     (is (->> (cluster/command-slot
                ["XREAD" "COUNT" 2 "STREAMS" key-a key-b "0" "0"]
                (route #'car/xread))
           (throws? :ex-info {:eid :carmine.cluster/cross-slot-keys})))
     (is (->> (cluster/command-slot
                ["MGET" (cluster/cluster-key key-a) (cluster/cluster-key key-b)] nil)
           (throws? :ex-info {:eid :carmine.cluster/cross-slot-keys})))
     (is (= (cluster/command-slot
              ["MIGRATE" "target.redis" 6379 key-a 0 1000]
              (route-info #'car/migrate))
           (cluster/cluster-slot (cluster/cluster-key key-a)))
       "MIGRATE's ordinary single-key form routes exactly")
     (is (= (cluster/command-slot
              ["MIGRATE" "target.redis" 6379 "" 0 1000
               "COPY" "AUTH" "KEYS" "KEYS" migrate-a migrate-b]
              (route-info #'car/migrate))
           (cluster/cluster-slot (cluster/cluster-key migrate-a)))
       "MIGRATE parses the final KEYS clause without confusing an AUTH password")
     (is (= (cluster/command-slot
              (write/prepare-command
                ["MIGRATE" "target.redis" 6379 (car/bytes (byte-array 0)) 0 1000
                 (car/bytes (enc/str->utf8-ba "COPY"))
                 (car/bytes (enc/str->utf8-ba "AUTH2"))
                 (car/bytes (enc/str->utf8-ba "KEYS"))
                 (car/bytes (enc/str->utf8-ba "KEYS"))
                 (car/bytes (enc/str->utf8-ba "REPLACE"))
                 (car/bytes (enc/str->utf8-ba "KEYS"))
                 migrate-a migrate-b])
              (route-info #'car/migrate))
           (cluster/cluster-slot (cluster/cluster-key migrate-a)))
       "MIGRATE parses exact wire arguments after enqueue preparation")
     (is (->> (cluster/command-slot
                ["MIGRATE" "target.redis" 6379 "" 0 1000
                 "KEYS" key-a key-b]
                (route-info #'car/migrate))
           (throws? :ex-info {:eid :carmine.cluster/cross-slot-keys})))
     (is (->> (cluster/command-slot
                (write/prepare-command
                  ["MIGRATE" "target.redis" 6379 (car/bytes (byte-array 0)) 0 1000
                   (car/bytes (enc/str->utf8-ba "KEYS")) key-a key-b])
                (route-info #'car/migrate))
           (throws? :ex-info {:eid :carmine.cluster/cross-slot-keys})))
     (is (nil? (cluster/command-slot
                 (write/prepare-command
                   ["XREAD" (car/bytes (enc/str->utf8-ba "ſTREAMS")) key-a "0"])
                 (route #'car/xread)))
       "Non-ASCII confusables are not folded into Redis command tokens")
     (is (= (cluster/command-slot
              (write/prepare-command
                ["MIGRATE" "target.redis" 6379 (byte-array 0) 0 1000
                 "KEYS" migrate-a])
              (route-info #'car/migrate))
           (cluster/command-slot
             (write/prepare-command ["GET" (byte-array 0)]) (route #'car/get)))
       "Auto-frozen empty arrays remain nonempty Redis keys")
     (is (= (cluster/command-slot ["SORT" key-a "ALPHA"] (route #'car/sort))
           (cluster/cluster-slot (cluster/cluster-key key-a)))
       "Partially specified SORT still routes by its known source key")]

    (let [spec (cluster/cluster-spec [["seed.redis" 7000]]
                 {:topology-source :cluster-slots})
          addr-a ["a.redis" 7000]
          addr-b ["b.redis" 7001]
          borrows_ (atom [])]
      (with-redefs-fn
        {#'cluster/raw-topology-reply!
         (fn [& _]
           [[0 8191 ["a.redis" 7000 "id-a"]]
            [8192 16383 ["b.redis" 7001 "id-b"]]])}
        #(cluster/refresh-topology! spec))
      (with-open [mgr
                  (conns/conn-manager-clustered
                    {:conn-opts {:server {:cluster-spec spec}}
                     :pool-opts {:test-while-idle? false}})]
        (with-redefs-fn
          {#'cluster/borrow-addr!
           (fn [_ addr f]
             (swap! borrows_ conj addr)
             (let [out (java.io.BufferedOutputStream.
                         (java.io.ByteArrayOutputStream.))]
               (f nil
                 (com/str->in
                   (str "+" (if (= addr addr-a) "A" "B") "\r\n"))
                 out)))}
          (fn []
            [(is (= (wcar mgr :as-vec
                      (car/set key-a "1")
                      (car/get key-b))
                    ["A" "B"]))
             (is (= @borrows_ [addr-a addr-b]))
             (reset! borrows_ [])
             (is (= (wcar mgr :as-vec (car/ping)) ["A"]))
             (is (= @borrows_ [addr-a]))
             (reset! borrows_ [])
             (is (= (wcar mgr
                      (car/with-cluster-target {:node-id "id-b"} (car/ping)))
                    "B"))
             (is (= @borrows_ [addr-b]))
             (reset! borrows_ [])
             (is (= (wcar mgr
                      (car/with-cluster-target {:addr addr-b} (car/ping)))
                    "B"))
             (is (= @borrows_ [addr-b]))
             (reset! borrows_ [])
             (is (= (wcar mgr
                      (car/with-cluster-target {:key key-b} (car/ping)))
                    "B"))
             (is (= @borrows_ [addr-b]))
             (reset! borrows_ [])
             (is (->> (wcar mgr (car/multi))
                   (throws? :ex-info
                     {:eid :carmine.cluster/unsupported-command})))
             (is (empty? @borrows_))
             (is (->> (wcar mgr (car/acl "WHOAMI"))
                   (throws? :ex-info {:eid :carmine.cluster/target-required})))
             (is (= (wcar mgr :as-vec
                      (car/with-cluster-target :nodes (car/echo "x")))
                    [[{:source {:node-id "id-a", :role :master, :addr addr-a}
                       :reply "A"}
                      {:source {:node-id "id-b", :role :master, :addr addr-b}
                       :reply "B"}]]))
             (is (->> (wcar mgr
                         (car/with-cluster-target {:slot 0}
                           (car/get key-b)))
                   (throws? :ex-info {:eid :carmine.cluster/target-conflict})))]))))))

(deftest _cluster-errors
  (let [reply-error
        (fn [message]
          (com/reply-error "[Carmine] Redis replied with an error"
            {:eid :carmine.read/redis-error-reply
             :message message
             :code (re-find #"^\S+" message)}))
        moved (reply-error "MOVED 3999 target.redis:6381")
        parsed (cluster/parse-cluster-error ["source.redis" 6379] moved)]
    [(is (= (dissoc parsed :error)
           {:kind :moved
            :transient? false
            :slot 3999
            :host "target.redis"
            :port 6381
            :addr ["target.redis" 6381]
            :message "MOVED 3999 target.redis:6381"}))
     (is (identical? (:error parsed) moved))

     (is (truss/submap?
           (cluster/parse-cluster-error ["source.redis" 6379]
             (reply-error "MOVED 0 :6380"))
           {:kind :moved, :slot 0, :host "source.redis", :port 6380
            :addr ["source.redis" 6380]}))

     (is (truss/submap?
           (cluster/parse-cluster-error (reply-error "MOVED 0 :6380"))
           {:kind :moved, :slot 0, :host nil, :port 6380, :addr nil})
       "A source-relative redirect remains valid without source context")

     (is (= (:addr
              (cluster/parse-cluster-error
                (reply-error "MOVED 16383 [::1]:6381")))
           ["::1" 6381]))
     (is (= (:addr
              (cluster/parse-cluster-error
                (reply-error "MOVED 16383 ::1:6381")))
           ["::1" 6381]))

     (is (truss/submap?
           (cluster/parse-cluster-error
             (reply-error "ASK 7 target.redis:6382"))
           {:kind :ask, :transient? true, :slot 7
            :addr ["target.redis" 6382]}))

     (is (truss/submap?
           (cluster/parse-cluster-error
             (reply-error "TRYAGAIN Multiple keys request during rehashing"))
           {:kind :try-again, :transient? true}))
     (is (truss/submap?
           (cluster/parse-cluster-error
             (reply-error "CLUSTERDOWN The cluster is down"))
           {:kind :cluster-down, :transient? true}))
     (is (truss/submap?
           (cluster/parse-cluster-error
             (reply-error "CROSSSLOT Keys in request do not hash to the same slot"))
           {:kind :cross-slot, :transient? false}))

     (is (nil? (cluster/parse-cluster-error (reply-error "ERR ordinary error"))))
     (is (nil? (cluster/parse-cluster-error
                 (com/reply-error "Parser error" {:eid :carmine.read/parser-error}))))
     (is (nil? (cluster/parse-cluster-error "MOVED 1 target.redis:6381")))

     (doseq [message
             ["MOVED 1"
              "MOVED 1 target.redis:6381 extra"
              "MOVED -1 target.redis:6381"
              "MOVED 16384 target.redis:6381"
              "MOVED 1 target.redis"
              "MOVED 1 target.redis:"
              "MOVED 1 target.redis:0"
              "MOVED 1 target.redis:65536"
              "MOVED 1 []:6381"
              "MOVED 1 [::1:6381"]]
       (is (->> (cluster/parse-cluster-error (reply-error message))
             (throws? :ex-info {:eid :carmine.cluster/invalid-redirection}))
         message))]))
