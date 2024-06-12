(ns ^:no-doc taoensso.carmine-v4.resp.write
  "Private ns, implementation detail."
  (:require
   [taoensso.encore :as enc]
   [taoensso.nippy  :as nippy]
   [taoensso.carmine-v4.resp.common :as com
    :refer [with-out with-out->str]])

  (:import
   [java.nio.charset StandardCharsets]
   [java.io BufferedOutputStream]))

(enc/declare-remote
  ^:dynamic taoensso.carmine-v4/*auto-freeze?*
  ^:dynamic taoensso.carmine-v4/*freeze-opts*)

(alias 'core 'taoensso.carmine-v4)

(comment (remove-ns 'taoensso.carmine-v4.resp.write))

;;;; Bulk byte strings

(do
  (def ^:const min-num-to-cache (long Short/MIN_VALUE))
  (def ^:const max-num-to-cache (long Short/MAX_VALUE)))

;; Cache ba representation of common number bulks, etc.
(let [long->bytes (fn [n] (.getBytes (Long/toString n) StandardCharsets/UTF_8))
      create-cache ; {<n> ((fn [n])->ba)}
      (fn [n-cast from-n to-n f]
        (java.util.concurrent.ConcurrentHashMap. ^java.util.Map
          (persistent!
            (enc/reduce-n
              (fn [m n] (let [n (n-cast n)] (assoc! m n (f n))))
              (transient {}) from-n to-n))))

      b* (int \*)
      b$ (int \$)
      ba-crlf com/ba-crlf]

  (let [;; {<n> *<n><CRLF>} for common lengths
        ^java.util.concurrent.ConcurrentHashMap cache
        (create-cache long 0 256
          (fn [n]
            (let [n-as-ba (long->bytes n)]
              (com/xs->ba \* n-as-ba "\r\n"))))]

    (defn write-array-len
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

  (let [b-colon (int \:)
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
                ^bytes     len-as-ba (long->bytes len)]

            (.write out b-colon)
            (.write out n-as-ba 0 len)
            (.write out ba-crlf 0 2))))))

  (let [;; {<n> $<len><CRLF><n><CRLF} for common longs
        ^java.util.concurrent.ConcurrentHashMap cache
        (create-cache long min-num-to-cache (inc max-num-to-cache)
          (fn [n]
            (let [^bytes       n-as-ba (long->bytes n)
                  len (alength n-as-ba)
                  ^bytes     len-as-ba (long->bytes len)]

              (com/xs->ba \$ len-as-ba "\r\n" n-as-ba "\r\n"))))]

    (defn- write-bulk-long
      [^BufferedOutputStream out n]
      (let [n (long n)]
        (if-let [^bytes cached-ba (.get cache n)]
          (.write out cached-ba 0 (alength cached-ba))

          (let [^bytes       n-as-ba (long->bytes n)
                len (alength n-as-ba)
                ^bytes     len-as-ba (long->bytes len)]

            (.write out b$)
            (.write out len-as-ba 0 (alength len-as-ba))
            (.write out ba-crlf   0 2)

            (.write out n-as-ba   0 len)
            (.write out ba-crlf   0 2))))))

  (let [double->bytes (fn [n] (.getBytes (Double/toString n) StandardCharsets/UTF_8))

        ;; {<n> $<len><CRLF><n><CRLF} for common whole doubles
        ^java.util.concurrent.ConcurrentHashMap cache
        (create-cache double min-num-to-cache (inc max-num-to-cache)
          (fn [n]
            (let [^bytes       n-as-ba (double->bytes n)
                  len (alength n-as-ba)
                  ^bytes     len-as-ba (long->bytes len)]

              (com/xs->ba \$ len-as-ba "\r\n" n-as-ba "\r\n"))))]

    (defn- write-bulk-double
      [^BufferedOutputStream out n]
      (let [n (double n)]
        (if-let [^bytes cached-ba (.get cache n)]
          (.write out cached-ba 0 (alength cached-ba))

          (let [^bytes       n-as-ba (double->bytes n)
                len (alength n-as-ba)
                ^bytes     len-as-ba (long->bytes len)]

            (.write out b$)
            (.write out len-as-ba 0 (alength len-as-ba))
            (.write out ba-crlf   0 2)

            (.write out n-as-ba   0 len)
            (.write out ba-crlf   0 2)))))))

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
        {:eid :carmine.write/null-reserved
         :arg s}))))

(defn- write-bulk-str [^BufferedOutputStream out s]
  (reserve-null!                       s)
  (write-bulk-ba out (enc/str->utf8-ba s)))

;;;; Wrapper types
;; IRedisArg behaviour influenced by wrapping arguments, wrapping
;; must capture any relevant dynamic config at wrap time.
;;
;; Implementation detail:
;; We try to avoid lazily converting arguments to Redis byte strings
;; (i.e. while writing to out) if there's a chance the conversion
;; could fail (e.g. Nippy freeze).

(deftype ToBytes [ba])
(defn ^:public to-bytes
  "Wraps given bytes to ensure that they'll be written to Redis
  without any modifications (serialization, blob markers, etc.)."
  (^ToBytes [ba]
   (if (instance? ToBytes ba)
     ba
     (if (enc/bytes? ba)
       (ToBytes.     ba)
       (throw
         (ex-info "[Carmine] `to-bytes` expects a byte-array argument"
           {:eid :carmine.write/unsupported-arg-type
            :arg (enc/typed-val ba)})))))

  ;; => Vector for destructuring (undocumented)
  ([ba & more] (mapv to-bytes (cons ba more))))

(deftype ToFrozen [arg freeze-opts ?frozen-ba])
(defn ^:public to-frozen
  "Wraps given argument to ensure that it'll be written to Redis
  using Nippy serialization [1].

  Options:
    See `taoensso.nippy/freeze` for `freeze-opts` docs.
    By default, `*freeze-opts*` value will be used.

  See also `as-thawed` for thawing (deserialization).
  [1] Ref. <https://github.com/ptaoussanis/nippy>"

  (^ToFrozen [            x] (to-frozen core/*freeze-opts* x))
  (^ToFrozen [freeze-opts x]
   ;; We do eager freezing here since we can, and we'd prefer to
   ;; catch freezing errors early (rather than while writing to out).
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
             core/*freeze-opts*
             freeze-opts))]

     (mapv #(to-frozen freeze-opts %) (cons x more)))))

;;;; IRedisArg

(defprotocol ^:private IRedisArg
  "Internal protocol, not for public use or extension."
  (write-bulk-arg [x ^BufferedOutputStream out]
    "Writes given arbitrary Clojure argument to `out` as a Redis byte string."))

(def ^:private bulk-nil
  (with-out
    (write-bulk-len out               2)
    (.write         out com/ba-nil  0 2)
    (.write         out com/ba-crlf 0 2)))

(comment (enc/utf8-ba->str bulk-nil))

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
          (ex-info "[Carmine] Trying to send argument of non-native type to Redis while `*auto-freeze?` is false"
            {:eid :carmine.write/non-native-arg-type
             :arg (enc/typed-val arg)})))]

  (extend-protocol IRedisArg
    String               (write-bulk-arg [s  out] (write-bulk-str out            s))
    Character            (write-bulk-arg [c  out] (write-bulk-str out (.toString c)))
    clojure.lang.Keyword (write-bulk-arg [kw out] (write-bulk-str out (kw->str   kw)))

    ;; Redis doesn't currently seem to accept `write-simple-long` (at least
    ;; without RESP3 mode?) though this seems an unnecessary limitation?
    Long    (write-bulk-arg [n out] (write-bulk-long   out       n))
    Integer (write-bulk-arg [n out] (write-bulk-long   out       n))
    Short   (write-bulk-arg [n out] (write-bulk-long   out       n))
    Byte    (write-bulk-arg [n out] (write-bulk-long   out       n))
    Double  (write-bulk-arg [n out] (write-bulk-double out       n))
    Float   (write-bulk-arg [n out] (write-bulk-double out       n))
    ToBytes (write-bulk-arg [x out] (write-bulk-ba     out (.-ba x)))
    ToFrozen
    (write-bulk-arg [x out]
      (let [ba (or (.-?frozen-ba x) (nippy/freeze x (.-freeze-opts x)))]
        (if core/*auto-freeze?*
          (write-bulk-ba out ba-npy ba)
          (write-bulk-ba out        ba))))

    Object
    (write-bulk-arg [x out]
      (if core/*auto-freeze?*
        (write-bulk-ba out ba-npy (nippy/freeze x))
        (non-native-type!                       x)))

    nil
    (write-bulk-arg [x ^BufferedOutputStream out]
      (if core/*auto-freeze?*
        (.write out bulk-nil 0 bulk-nil-len)
        (non-native-type! x))))

  (extend-type (Class/forName "[B") ; Extra `extend` needed due to CLJ-1381
    IRedisArg
    (write-bulk-arg [ba out]
      (if core/*auto-freeze?*
        (write-bulk-ba out ba-bin ba) ; Write   marked bytes
        (write-bulk-ba out        ba) ; Write unmarked bytes
        ))))

;;;;

(defn- write-requests ; Used only for REPL/testing
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
