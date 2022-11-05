(ns taoensso.carmine.impl.resp.write
  "Write-side implementation of the Redis RESP3 protocol,
  Ref. https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.test    :as test :refer [deftest testing is]]
   [taoensso.encore :as enc  :refer [throws?]]
   [taoensso.nippy  :as nippy]
   [taoensso.carmine.impl.resp.common :as com
    :refer [str->bytes bytes->str with-out with-out->str]])

  (:import
   [java.nio.charset StandardCharsets]
   [java.io BufferedOutputStream]))

(comment
  (remove-ns      'taoensso.carmine.impl.resp.write)
  (test/run-tests 'taoensso.carmine.impl.resp.write))

(enc/declare-remote
  ^:dynamic taoensso.carmine-v4/*auto-serialize?*)

(alias 'core 'taoensso.carmine-v4)

;;;; Bulk byte strings

(do
  (def ^:private ^:const min-num-to-cache Short/MIN_VALUE)
  (def ^:private ^:const max-num-to-cache Short/MAX_VALUE)
  (def ^:private ^:const uncached-num     (inc max-num-to-cache)))

;; Cache ba representation of common number bulks, etc.
(let [long->bytes (fn [n] (.getBytes (Long/toString n) StandardCharsets/UTF_8))
      create-cache ; {<n> ((fn [n])->ba)}
      (fn [n-cast from-n to-n f]
        (java.util.concurrent.ConcurrentHashMap. ^java.util.Map
          (persistent!
            (enc/reduce-n
              (fn [m n] (let [n (n-cast n)] (assoc! m n (f n))))
              (transient {}) from-n to-n))))

      b* (byte \*)
      b$ (byte \$)
      ba-crlf com/ba-crlf]

  (let [;; {<n> *<n><CRLF>} for common lengths
        ^java.util.concurrent.ConcurrentHashMap cache
        (create-cache long 0 256
          (fn [n]
            (let [n-as-ba (long->bytes n)]
              (com/xs->ba \* n-as-ba "\r\n"))))]

    (defn- write-array-len
      [^BufferedOutputStream out n]
      (let [n (long n)]
        (if-let [^bytes cached-ba (.get cache n)]
          (.write out cached-ba 0 (alength cached-ba))

          (let [^bytes n-as-ba (long->bytes n)]
            (.write out b*)
            (.write out n-as-ba 0 (alength n-as-ba))
            (.write out ba-crlf 0 2))))))

  (let [;; {<n> $<n><CRLF>} for common lengths
        ^java.util.concurrent.ConcurrentHashMap cache
        (create-cache long 0 256
          (fn [n]
            (let [n-as-ba (long->bytes n)]
              (com/xs->ba \$ n-as-ba "\r\n"))))]

    (defn- write-bulk-len
      [^BufferedOutputStream out n]
      (let [n (long n)]
        (if-let [^bytes cached-ba (.get cache n)]
          (.write out cached-ba 0 (alength cached-ba))

          (let [^bytes n-as-ba (long->bytes n)]
            (.write out b$)
            (.write out n-as-ba 0 (alength n-as-ba))
            (.write out ba-crlf 0 2))))))

  (let [b-colon (byte \:)
        ;; {<n> :<n><CRLF>} for common longs
        ^java.util.concurrent.ConcurrentHashMap cache
        (create-cache long min-num-to-cache (inc max-num-to-cache)
          (fn [n] (com/xs->ba \: (long->bytes n) "\r\n")))]

    (defn- write-simple-long
      [^BufferedOutputStream out n]
      (let [n (long n)]
        (if-let [^bytes cached-ba (.get cache n)]
          (.write out cached-ba 0 (alength cached-ba))

          (let [^bytes       n-as-ba (long->bytes n)
                len (alength n-as-ba)

                ^bytes len-as-ba (long->bytes len)]

            (.write out b-colon)
            (.write out n-as-ba 0 len)
            (.write out ba-crlf 0 2))))))

  (let [double->bytes (fn [n] (.getBytes (Double/toString n) StandardCharsets/UTF_8))

        ;; {<n> $<len><CRLF><n><CRLF} for common whole doubles
        ^java.util.concurrent.ConcurrentHashMap cache
        (create-cache double min-num-to-cache (inc max-num-to-cache)
          (fn [n]
            (let [^bytes       n-as-ba (double->bytes n)
                  len (alength n-as-ba)

                  ^bytes len-as-ba (long->bytes len)]

              (com/xs->ba \$ len-as-ba "\r\n" n-as-ba "\r\n"))))]

    (defn- write-bulk-double
      [^BufferedOutputStream out n]
      (let [n (double n)]
        (if-let [^bytes cached-ba (.get cache n)]
          (.write out cached-ba 0 (alength cached-ba))

          (let [^bytes       n-as-ba (double->bytes n)
                len (alength n-as-ba)

                ^bytes len-as-ba (long->bytes len)]

            (.write out b$)
            (.write out len-as-ba 0 (alength len-as-ba))
            (.write out ba-crlf   0 2)

            (.write out n-as-ba   0 len)
            (.write out ba-crlf   0 2)))))))

(deftest ^:private _write-nums
  [(is (= (with-out->str (write-array-len out           12))    "*12\r\n"))
   (is (= (with-out->str (write-array-len out uncached-num)) "*32768\r\n"))

   (is (= (with-out->str (write-bulk-len out           12))    "$12\r\n"))
   (is (= (with-out->str (write-bulk-len out uncached-num)) "$32768\r\n"))

   (is (= (with-out->str (write-simple-long out           12))    ":12\r\n"))
   (is (= (with-out->str (write-simple-long out uncached-num)) ":32768\r\n"))

   (is (= (with-out->str (write-bulk-double out           12))    "$4\r\n12.0\r\n"))
   (is (= (with-out->str (write-bulk-double out uncached-num)) "$7\r\n32768.0\r\n"))])

(let [write-bulk-len write-bulk-len
      ba-crlf        com/ba-crlf]

  (defn- write-bulk-ba
    "$<len><CRLF><payload><CRLF>"
    ([^BufferedOutputStream out ^bytes ba]
     (let [len (alength ba)]
       (write-bulk-len   out           len)
       (.write           out ba      0 len)
       (.write           out ba-crlf 0   2)))

    ([^BufferedOutputStream out ^bytes ba-marker ^bytes ba-payload]
     (let [marker-len  (alength ba-marker)
           payload-len (alength ba-payload)
           total-len   (+ marker-len payload-len)]
       (write-bulk-len out                total-len)
       (.write         out ba-marker  0  marker-len)
       (.write         out ba-payload 0 payload-len)
       (.write         out ba-crlf    0           2)))))

(defn- reserve-null!
  "This is a Carmine (not Redis) limitation to support auto null-prefixed
  blob markers with special semantics (`ba-npy`, etc.)."
  [^String s]
  (when (and (not (.isEmpty s)) (== ^int (.charAt s 0) 0))
    (throw
      (ex-info "[Carmine] String args can't begin with null (char 0)"
        {:eid :carmine.resp.write/null-reserved
         :arg s}))))

(defn- write-bulk-str [^BufferedOutputStream out s]
  (reserve-null!                 s)
  (write-bulk-ba out (str->bytes s)))

(deftest ^:private _write-bulk-str
  [(is (= (with-out->str (write-bulk-str out "hello\r\n"))
         "$7\r\nhello\r\n\r\n"))

   (is (= (with-out->str (write-bulk-str out com/unicode-string))
         "$43\r\nಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ\r\n\r\n"))

   (testing "reserve-null!"
     [(is (nil?                                                     (reserve-null! "")))
      (is (throws? :common {:eid :carmine.resp.write/null-reserved} (reserve-null! "\u0000<")))])

   (testing "Bulk num/str equivalence"
     [(is (=
            (with-out->str (write-bulk-double out  12.5))
            (with-out->str (write-bulk-str    out "12.5"))))
      (is (=
            (with-out->str (write-bulk-double out      (double uncached-num)))
            (with-out->str (write-bulk-str    out (str (double uncached-num))))))])])

;;;; Wrapper types
;; IRedisArg behaviour influenced by wrapping arguments, wrapping
;; must capture any relevant dynamic config at wrap time.
;;
;; Implementation detail:
;; Try to avoid lazily converting arguments to Redis byte strings
;; (i.e. while writing to out) if there's a chance the conversion
;; could fail (e.g. Nippy freeze).

(deftype ToBytes [ba])
(defn to-bytes
  "Wraps given bytes to ensure that they'll be written to Redis
  without any modifications (serialization, blob markers, etc.)."
  (^ToBytes [ba]
   (if (instance? ToBytes ba)
     ba
     (if (enc/bytes? ba)
       (ToBytes.     ba)
       (throw
         (ex-info "[Carmine] `to-bytes` expects a byte-array argument"
           {:eid :carmine.resp.write/unsupported-arg-type
            :arg {:type (type ba) :value ba}})))))

  ;; => Vector for destructuring (undocumented)
  ([ba & more] (mapv to-bytes (cons ba more))))

;;; TODO Docstrings
(def ^:dynamic *freeze-opts* nil)
(deftype ToFrozen [arg freeze-opts ?frozen-ba])
(defn to-frozen
  ;; We do eager freezing here since we can, and we'd prefer to
  ;; catch freezing errors early (rather than while writing to out).
  (^ToFrozen [            x] (to-frozen *freeze-opts* x))
  (^ToFrozen [freeze-opts x]
   (if (instance? ToFrozen x)
     (let [^ToFrozen x x]
       (if (= freeze-opts (.-freeze-opts x))
         x
         (let [arg (.-arg x)]
           ;; Re-freeze (expensive)
           (ToFrozen. arg freeze-opts
             (nippy/freeze arg freeze-opts)))))

     (ToFrozen. x freeze-opts
       (nippy/freeze x freeze-opts))))

  ;; => Vector for destructuring (undocumented)
  ([freeze-opts x & more]
   (let [freeze-opts
         (enc/have [:or nil? map?]
           (if (identical? freeze-opts :dynamic)
             *freeze-opts*
             freeze-opts))]

     (mapv #(to-frozen freeze-opts %) (cons x more)))))

(deftest ^:private _wrappers
  [(is (= (bytes->str (.-ba (to-bytes (to-bytes (com/xs->ba [\a \b \c]))))) "abc"))
   (is (= (.-freeze-opts (to-frozen {:a :A} (to-frozen {:b :B} "x"))) {:a :A}))

   (is (= (binding [*freeze-opts* {:o :O}]
            (let [[c1 c2 c3] (to-frozen :dynamic "x" "y" "z")]
              (mapv #(.-freeze-opts ^ToFrozen %) [c1 c2 c3])))
         [{:o :O} {:o :O} {:o :O}])
     "Multiple frozen arguments sharing dynamic config")])

;;;; IRedisArg

(defprotocol IRedisArg
  (write-bulk-arg [x ^BufferedOutputStream out]
    "Writes given arbitrary Clojure argument to `out` as a Redis byte string."))

(def ^:private bulk-nil
  (with-out
    (write-bulk-len out               2)
    (.write         out com/ba-nil  0 2)
    (.write         out com/ba-crlf 0 2)))

(comment (bytes->str bulk-nil))

(let [write-bulk-str write-bulk-str
      ba-bin       com/ba-bin
      ba-npy       com/ba-npy
      bulk-nil     bulk-nil
      bulk-nil-len (alength ^bytes bulk-nil)
      kw->str
      (fn [kw]
        (if-let [ns (namespace kw)]
          (str ns "/" (name kw))
          (do         (name kw))))

      non-native-type!
      (fn [arg]
        (throw
          (ex-info "[Carmine] Trying to send argument of non-native type to Redis while `*auto-serialize?` is false"
            {:eid :carmine.resp.write/non-native-arg-type
             :arg {:type (type arg) :value arg}})))]

  (extend-protocol IRedisArg

    String               (write-bulk-arg [s  out] (write-bulk-str out            s))
    Character            (write-bulk-arg [c  out] (write-bulk-str out (.toString c)))
    clojure.lang.Keyword (write-bulk-arg [kw out] (write-bulk-str out (kw->str   kw)))

    Long    (write-bulk-arg [n out] (write-simple-long out       n))
    Integer (write-bulk-arg [n out] (write-simple-long out       n))
    Short   (write-bulk-arg [n out] (write-simple-long out       n))
    Byte    (write-bulk-arg [n out] (write-simple-long out       n))
    Double  (write-bulk-arg [n out] (write-bulk-double out       n))
    Float   (write-bulk-arg [n out] (write-bulk-double out       n))
    ToBytes (write-bulk-arg [x out] (write-bulk-ba     out (.-ba x)))
    ToFrozen
    (write-bulk-arg [x out]
      (let [ba (or (.-?frozen-ba x) (nippy/freeze x (.-freeze-opts x)))]
        (if core/*auto-serialize?*
          (write-bulk-ba out ba-npy ba)
          (write-bulk-ba out        ba))))

    Object
    (write-bulk-arg [x out]
      (if core/*auto-serialize?*
        (write-bulk-ba out ba-npy (nippy/freeze x))
        (non-native-type!                       x)))

    nil
    (write-bulk-arg [x ^BufferedOutputStream out]
      (if core/*auto-serialize?*
        (.write out bulk-nil 0 bulk-nil-len)
        (non-native-type! x))))

  (extend-type (Class/forName "[B") ; Extra `extend` needed due to CLJ-1381
    IRedisArg
    (write-bulk-arg [ba out]
      (if core/*auto-serialize?*
        (write-bulk-ba out ba-bin ba) ; Write   marked bytes
        (write-bulk-ba out        ba) ; Write unmarked bytes
        ))))

;;;;

(defn write-requests
  "Sends pipelined requests to Redis server using its byte string protocol:
      *<num of args> crlf
        [$<size of arg> crlf
          <arg payload> crlf ...]"
  [^BufferedOutputStream out reqs]
  (enc/run!
    (fn [req-args]
      (let [n-args (count req-args)]
        (when-not (== n-args 0)
          (write-array-len out n-args)
          (enc/run!
            (fn [arg] (write-bulk-arg arg out))
            req-args))))
    reqs)
  (.flush out))

(deftest ^:private _write-requests
  [(testing "Basics"
     [(is (= (bytes->str bulk-nil) "$2\r\n\u0000_\r\n"))
      (is (= (with-out->str (write-requests out [["hello\r\n"]]))        "*1\r\n$7\r\nhello\r\n\r\n"))
      (is (= (with-out->str (write-requests out [[com/unicode-string]])) "*1\r\n$43\r\nಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ\r\n\r\n"))
      (is (=
            (with-out->str
              (write-requests out [["a1" "a2" "a3"] ["b1"] ["c1" "c2"]]))
            "*3\r\n$2\r\na1\r\n$2\r\na2\r\n$2\r\na3\r\n*1\r\n$2\r\nb1\r\n*2\r\n$2\r\nc1\r\n$2\r\nc2\r\n")

        "Multiple reqs, with multiple args each")

      (is (= (with-out->str (write-requests out [["str" 1 2 3 4.0 :kw \x]]))
            "*7\r\n$3\r\nstr\r\n:1\r\n:2\r\n:3\r\n$3\r\n4.0\r\n$2\r\nkw\r\n$1\r\nx\r\n"))])

   (testing "Blob markers"
     [(testing "Auto serialization enabled"
        (binding [core/*auto-serialize?* true]
          [(is (= (with-out->str (write-requests out [[nil]])) "*1\r\n$2\r\n\u0000_\r\n")            "nil arg => ba-nil marker")
           (is (= (with-out->str (write-requests out [[{}]]))  "*1\r\n$7\r\n\u0000>NPY\u0000\r\n") "clj arg => ba-npy marker")

           (let [ba (byte-array [(byte \a) (byte \b) (byte \c)])]
             [(is (= (with-out->str (write-requests out [[          ba]]))  "*1\r\n$5\r\n\u0000<abc\r\n") "ba-bin marker")
              (is (= (with-out->str (write-requests out [[(to-bytes ba)]])) "*1\r\n$3\r\nabc\r\n") "Unmarked bin")])]))

      (testing "Auto serialization disabled"
        (binding [core/*auto-serialize?* false]
          (let [pattern {:eid :carmine.resp.write/non-native-arg-type}]
           [(is (throws? :common pattern (with-out->str (write-requests out [[nil]]))) "nil arg => throw")
            (is (throws? :common pattern (with-out->str (write-requests out [[{}]])))  "clj arg => throw")

            (let [ba (byte-array [(byte \a) (byte \b) (byte \c)])]
              [(is (= (with-out->str (write-requests out [[          ba]]))  "*1\r\n$3\r\nabc\r\n") "Unmarked bin")
               (is (= (with-out->str (write-requests out [[(to-bytes ba)]])) "*1\r\n$3\r\nabc\r\n") "Same unmarked bin with `to-bytes`")])])))])])
