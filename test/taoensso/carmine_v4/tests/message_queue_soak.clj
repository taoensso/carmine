(ns taoensso.carmine-v4.tests.message-queue-soak
  "Opt-in mixed core/MQ canary (`lein test-v4-canary`)."
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is]]
   [taoensso.carmine-v4 :as car]
   [taoensso.carmine-v4.conns :as conns]
   [taoensso.carmine-v4.message-queue :as mq]
   [taoensso.carmine-v4.tests.mq-soak :as soak]))

(defn- env-long [k default]
  (Long/parseLong (or (not-empty (System/getenv k)) (str default))))

(defn- env-bool [k default]
  (if-let [value (System/getenv k)]
    (contains? #{"1" "true" "yes" "on"} (str/lower-case value))
    default))

(defn- start-core-load! [manager run-id concurrency]
  (let [running_ (atom true)
        ops_ (atom 0)
        error-count_ (atom 0)
        errors_ (atom [])
        keys (mapv #(str "carmine:v4:canary:" run-id ":" %) (range concurrency))
        workers
        (mapv
          (fn [idx]
            (future
              (let [key (nth keys idx)
                    counter-key (str key ":counter")]
                (loop [iteration 0]
                  (when @running_
                    (let [value (str run-id ":" idx ":" iteration)]
                      (try
                        (let [[set-reply get-reply n lua-reply]
                              (car/wcar manager {:as-vec? true}
                                (car/set key value)
                                (car/get key)
                                (car/incr counter-key)
                                (car/lua "return redis.call('get', _:key)"
                                  {:key key} {}))]
                          (when-not (and (= set-reply "OK") (= get-reply value)
                                      (pos-int? n) (= lua-reply value))
                            (throw (ex-info "Unexpected canary pipeline reply"
                                     {:replies [set-reply get-reply n lua-reply]})))
                          (swap! ops_ inc))
                        (catch Throwable t
                          (swap! error-count_ inc)
                          (swap! errors_
                            #(if (< (count %) 100)
                               (conj % {:worker idx, :iteration iteration
                                        :class (.getName (class t))
                                        :message (.getMessage t)})
                               %))
                          (Thread/sleep 10)))
                      (recur (inc iteration))))))))
          (range concurrency))]
    {:stop!
     (fn [timeout-ms]
       (reset! running_ false)
       (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
         (doseq [worker workers]
           (when (= (deref worker
                      (max 0 (- deadline (System/currentTimeMillis))) ::timeout)
                    ::timeout)
             (future-cancel worker)
             (swap! error-count_ inc)
             (swap! errors_ conj {:error :core-worker-stop-timeout}))))
       {:operations @ops_, :error-count @error-count_, :errors @errors_})
     :keys (into keys (map #(str % ":counter") keys))}))

(defn- remaining-ms [deadline]
  (max 0 (- deadline (System/currentTimeMillis))))

(defn- run-before! [deadline description f]
  (let [task (future (f))
        timeout (Object.)
        result (deref task (remaining-ms deadline) timeout)]
    (if (identical? result timeout)
      (do
        (future-cancel task)
        (throw (ex-info (str "Timed out during " description)
                 {:description description})))
      result)))

(deftest _final-enqueue-retries-an-ambiguous-commit-idempotently
  (let [calls_ (atom [])
        controller
        (soak/map->Soak
          {:queue ::queue, :mid-count 1, :run-id "deterministic-run"
           :revisions_ (atom {0 7}), :enqueue-locks [(Object.)]})]
    (with-redefs [mq/msg-enqueue!
                  (fn [queue msg opts]
                    (let [call [queue msg opts]
                          attempt (count (swap! calls_ conj call))]
                      (if (= attempt 1)
                        (throw (ex-info "Ambiguous committed enqueue" {:injected? true}))
                        {:success? true, :action :existing, :mid "0"})))]
      (is (= (#'soak/enqueue-final! controller 0 1000) 8))
      (is (= @calls_
            (repeat 2
              [::queue
               {:logical-mid 0, :revision 8, :final? true
                :proof "deterministic-run:0:8"}
               {:mid "0", :revision 8}]))
        "The replay must reuse the exact committed revision and payload"))))

(deftest ^{:mq-soak true, :v4-canary true} _mixed-release-canary
  (let [redis-uri (not-empty (System/getenv "CARMINE_TEST_REDIS_URI"))
        redis-port (env-long "CARMINE_TEST_REDIS_PORT" 6379)
        resp3? (env-bool "CARMINE_TEST_REDIS_RESP3" true)
        manager-opts
        {:conn-opts
         {:server (or redis-uri ["127.0.0.1" redis-port])
          :init {:resp3? resp3?}}}
        config
        {:seconds (env-long "CARMINE_MQ_SOAK_SECONDS" 30)
         :mid-count (env-long "CARMINE_MQ_SOAK_MIDS" 32)
         :producer-count (env-long "CARMINE_MQ_SOAK_PRODUCERS" 4)
         :mq-concurrency (env-long "CARMINE_MQ_SOAK_CONCURRENCY" 8)
         :core-concurrency (env-long "CARMINE_V4_CANARY_CORE_CONCURRENCY" 4)
         :timeout-seconds (env-long "CARMINE_MQ_SOAK_TIMEOUT_SECONDS" 300)
         :endpoint (if redis-uri :configured-uri :local)
         :resp3? resp3?}
        run-id (str (java.util.UUID/randomUUID))
        timeout-ms (* 1000 (:timeout-seconds config))
        manager (conns/conn-manager-pooled manager-opts)
        controller_ (volatile! nil)
        core-load_ (volatile! nil)]
    (try
      (let [controller (soak/start! manager "standalone-canary"
                         {:mid-count (:mid-count config)
                          :producer-count (:producer-count config)
                          :concurrency (:mq-concurrency config)})
            _ (vreset! controller_ controller)
            core-load (start-core-load! manager run-id (:core-concurrency config))
            _ (vreset! core-load_ core-load)]
      (try
        (Thread/sleep (long (* 1000 (:seconds config))))
        (let [core-report ((:stop! core-load) timeout-ms)
              mq-report (soak/finish! controller timeout-ms)
              report
              {:schema-version 1, :run-id run-id, :config config
               :core core-report, :mq mq-report
               :manager-stats (car/conn-manager-stats manager)}]
          (println "\nCarmine v4 release canary report:")
          (prn report)
          [(is (pos? (:operations core-report)))
           (is (zero? (:error-count core-report)))
           (is (:ordered? mq-report))
           (is (:no-phantoms? mq-report))
           (is (:drained? mq-report))
           (is (:successors-exercised? mq-report))
           (is (:successors-bounded? mq-report))
           (is (zero? (:producer-errors mq-report))
             (str "Unexpected producer errors in the standalone MQ canary: "
               (pr-str {:producer-errors (:producer-errors mq-report)
                        :worker-stats (:worker-stats mq-report)
                        :config config})))])
        (finally ((:stop! core-load) (min 1000 timeout-ms)))))
      (finally
        (let [deadline (+ (System/currentTimeMillis) timeout-ms)
              controller @controller_
              core-load @core-load_]
          (try
            (when controller
              (run-before! deadline "MQ canary cleanup"
                #(soak/close! controller)))
            (finally
              (try
                (when core-load
                  (run-before! deadline "core canary key cleanup"
                    #(car/wcar manager
                       (doseq [key (:keys core-load)] (car/del key)))))
                (finally
                  (car/conn-manager-close! manager (remaining-ms deadline)
                    {:via 'mixed-release-canary}))))))))))
