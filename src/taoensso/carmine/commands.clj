(ns ^:no-doc taoensso.carmine.commands
  "Macros to define an up-to-date, fully documented function for every Redis
  command as specified in the official json command spec."
  (:require [clojure.string  :as str]
            [taoensso.encore :as enc]
            [taoensso.carmine.protocol :as protocol])
  (:import  [taoensso.carmine.protocol EnqueuedRequest Context]))

;;;; Cluster keyslots

(def ^:private ^:const num-keyslots 16384)
(let [xmodem-crc16-lookup
      (long-array
        [0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
         0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
         0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
         0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
         0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
         0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
         0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
         0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
         0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
         0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
         0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
         0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
         0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
         0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
         0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
         0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
         0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
         0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
         0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
         0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
         0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
         0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
         0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
         0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
         0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
         0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
         0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
         0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
         0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
         0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
         0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
         0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0])]

  (defn- crc16
    "Thanks to Ben Poweski for our implementation here."
    [^bytes ba]
    (let [len (alength ba)]
      (loop [n   0
             crc 0]
        (if (>= n len)
          crc
          (recur (inc n)
            (bit-xor (bit-and (bit-shift-left crc 8) 0xffff)
              (aget xmodem-crc16-lookup
                (-> (bit-shift-right crc 8)
                  (bit-xor (aget ba n))
                  (bit-and 0xff))))))))))

(defprotocol IKeySlot
  "Returns the Redis Cluster key slot ℕ∈[0,num-keyslots) for given key arg
  using the CRC16 algorithm, Ref. http://redis.io/topics/cluster-spec (Appendix A)."
  (keyslot [redis-key]))

(extend-type (Class/forName "[B")
  IKeySlot (keyslot [rk] (mod (crc16 rk) num-keyslots)))

(extend-type String
  IKeySlot
  (keyslot [rk]
    (let [match
          (when (enc/str-contains? rk "{")
            (re-find #"\{(.*?)\}" rk))

          ^String to-hash
          (if match
            (let [tag (nth match 1)] ; "bar" in "foo{bar}{baz}"
              (if (.isEmpty ^String tag) rk tag))
            rk)]

      (mod (crc16 (.getBytes to-hash "UTF-8")) num-keyslots))))

(comment
  [(re-find #"\{(.*?)\}" "foo{bar}{baz}")
   (re-find #"\{(.*?)\}" "foo")]
  [(keyslot "foobar") (keyslot "ignore-this{foobar}")]
  (enc/qb 1e5
    (keyslot "hello")
    (keyslot "hello{world}")) ; [14.69 56.41]
  )

;;;; Command specs

(comment ; Generate commands.edn
  (require '[clojure.data.json :as json])
  (defn- get-redis-command-spec
    [source]
    (let [json
          (case source
            :online (slurp (java.net.URL. "https://raw.githubusercontent.com/redis/redis-doc/master/commands.json"))
            :local  (enc/slurp-resource "redis-commands.json"))]

      {:as-map  (clojure.data.json/read-str json :key-fn keyword),
       :as-json json}))

  (comment (= (get-redis-command-spec :local) (get-redis-command-spec :online)))

  (defn- get-fixed-params [^long n-min arguments]
    (let [simple-params
          (reduce
            (fn [acc in]
              (let [{:keys [optional multiple arguments name]} (enc/have map? in)]
                (cond
                  optional  (reduced       acc)
                  arguments (reduced       acc)
                  multiple  (reduced (conj acc (symbol name)))
                  :else              (conj acc (symbol name)))))
            []
            arguments)

          n-additional (- n-min (count simple-params))]

      (if (pos? n-additional)
        (into simple-params (mapv #(symbol (str "arg" %)) (range 1 (inc n-additional))))
        (do   simple-params))))

  (defn- get-carmine-command-spec
    [redis-command-spec]
    (try
      (let [as-map
            (persistent!
              (reduce-kv
                (fn [m k v]
                  (let [cmd-name (name k)                                            ; "CONFIG SET"
                        cmd-args (-> cmd-name (str/split #" "))                      ; ["CONFIG" "SET"]
                        fn-name  (-> cmd-name str/lower-case (str/replace #" " "-")) ; "config-set"

                        {:keys [summary since complexity arguments arity _group]} v

                        ;; Ref. https://redis.io/commands/command/
                        [fn-params-fixed fn-params-more req-args-fixed]
                        (let [n     (long arity)
                              more? (neg?     n)
                              n
                              (if more?
                                (+ n (count cmd-args))
                                (- n (count cmd-args)))

                              n-min (Math/abs n)
                              fixed (get-fixed-params n-min arguments)]
                          [fixed (when more? (into fixed '[& args])) (into cmd-args fixed)])

                        fn-docstring
                        (let [docs-url (str "https://redis.io/commands/" fn-name "/")]
                          (enc/into-str
                            "`" cmd-name "` - Redis command function.\n"
                            (when since      ["  Available since: Redis " since      "\n"])
                            (when complexity ["       Complexity: "       complexity "\n"])
                            "\n" summary
                            "\n"
                            "Ref. " docs-url " for more info."))

                        ;; Assuming for now that cluster key always follows
                        ;; right after command args (seems to hold?).
                        ;; Can adjust later if needed.
                        cluster-key-idx (count cmd-args)]

                    (enc/have? pos? cluster-key-idx)
                    (assoc! m cmd-name
                      {:fn-name         fn-name
                       :cluster-key-idx cluster-key-idx
                       :fn-params-more  fn-params-more
                       :fn-params-fixed fn-params-fixed
                       :req-args-fixed  req-args-fixed ; ["CONFIG" "SET" 'key 'value]
                       :fn-docstring    fn-docstring})))

                (transient {})
                (get redis-command-spec :as-map)))]

        {:as-map as-map
         :as-edn
         (str
           "{\n"
           (reduce
             (fn [acc k]
               (let [v (get as-map k)]
                 (str acc (enc/pr-edn k) " " (enc/pr-edn v) "\n")))
             ""
             (sort (keys as-map)))
           "}")})

      (catch Throwable t
        (throw
          (ex-info "Failed to generate Carmine command spec"
            {:redis-command-spec redis-command-spec}
            t)))))

  (comment
    (get-in                           (get-redis-command-spec :local)  [:as-map :XTRIM])
    (get-in (get-carmine-command-spec (get-redis-command-spec :local)) [:as-map "XTRIM"]))

  (defn update-commands! [json-source]
    (let [redis-command-spec   (get-redis-command-spec   json-source)
          carmine-command-spec (get-carmine-command-spec redis-command-spec)]

      (spit "resources/redis-commands.json"  (enc/have (:as-json redis-command-spec)))
      (spit "resources/carmine-commands.edn" (enc/have (:as-edn  carmine-command-spec)))))

  (update-commands! :local)
  (update-commands! :online))

;;;;

(defn enqueue-request
  "Implementation detail.
  Takes a request like [\"SET\" \"my-key\" \"my-val\"] and adds it to
  dynamic context's request queue."
  ([cluster-key-idx request more-args]
   (enqueue-request cluster-key-idx
     (reduce conj request more-args) ; Avoid transients
     #_(into      request more-args)))

  ([cluster-key-idx request]
   ;; (enc/have? vector? request)
   (let [context protocol/*context*
         _ (when (nil? context) (throw protocol/no-context-ex))
         parser  protocol/*parser*
         ^Context context context
         conn       (.-conn       context)
         req-queue_ (.-req-queue_ context)
         ;; cluster-mode? (.-cluster-mode? context)
         cluster-mode? false #_(get-in conn [:spec :cluster])

         request-bs (mapv protocol/byte-str request)
         cluster-keyslot
         (if cluster-mode?
           (let [ck (nth request cluster-key-idx)]
             (if (string? ck)
               (keyslot ck)
               (keyslot (nth request-bs cluster-key-idx))))
           0)

         ereq (EnqueuedRequest. cluster-keyslot parser request request-bs)]

     (swap! req-queue_ conj ereq))))

;;;;

(def ^:private skip-fns "#{<fn-name>}" #{})
(def ^:private rename-fns "{<from-fn-name> <to-fn-name>}" {})

(defmacro defcommand [cmd-name spec]
  (let [{:keys [fn-name fn-docstring fn-params-fixed fn-params-more
                req-args-fixed cluster-key-idx]} spec]

    ;; TODO Optimization: could pre-generate raw byte-strings for req
    ;; cmd args, e.g. ["CONFIG" "SET"]?

    (when-not (skip-fns fn-name)
      (let [fn-name (get rename-fns fn-name fn-name)]
        (if fn-params-more
          `(defn ~(symbol fn-name) ~fn-docstring {:redis-api true}
             ~`(~fn-params-fixed (enqueue-request ~cluster-key-idx ~req-args-fixed))
             ~`(~fn-params-more  (enqueue-request ~cluster-key-idx ~req-args-fixed ~'args)))

          `(defn ~(symbol fn-name) ~fn-docstring {:redis-api true}
             ~fn-params-fixed
             (enqueue-request ~cluster-key-idx ~req-args-fixed)))))))

(comment
  (count command-spec) ; 197
  (get command-spec "HMGET")
  (macroexpand
    '(defcommand "HMGET"
       {:fn-name "hmget", :fn-docstring "doc", :fn-params-fixed [key field],
        :fn-params-more [key field & args], :req-args-fixed ["HMGET" key field],
        :cluster-key-idx 1})))

(defonce ^:private command-spec
  (if-let [edn (enc/slurp-resource "carmine-commands.edn")]
    (try
      (enc/read-edn edn)
      (catch Exception e
        (throw (ex-info "Failed to read Carmine commands edn" {} e))))
    (throw (ex-info "Failed to find Carmine commands edn" {}))))

(defmacro defcommands []
  `(do ~@(map (fn [[k v]] `(defcommand ~k ~v)) command-spec)))

(comment (defcommands))
