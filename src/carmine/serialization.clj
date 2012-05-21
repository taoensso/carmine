(ns carmine.serialization
  "Deliberately simple, high-performance de/serializer for Clojure. Adapted from
  Deep-Freeze."
  (:import [java.io DataInputStream DataOutputStream ByteArrayOutputStream
            ByteArrayInputStream]
           [org.xerial.snappy Snappy]))

;;;; Define type IDs

(def ^:const id-reader  (int 0)) ; Fallback: *print-dup* pr-str output
(def ^:const id-bytes   (int 1))
(def ^:const id-nil     (int 2))
(def ^:const id-boolean (int 3))

(def ^:const id-char    (int 10))
(def ^:const id-string  (int 11))
(def ^:const id-keyword (int 12))

(def ^:const id-list    (int 20))
(def ^:const id-vector  (int 21))
(def ^:const id-map     (int 22))
(def ^:const id-set     (int 23))
(def ^:const id-coll    (int 24)) ; Non-specific collection
(def ^:const id-meta    (int 25))

(def ^:const id-byte    (int 40))
(def ^:const id-short   (int 41))
(def ^:const id-integer (int 42))
(def ^:const id-long    (int 43))
(def ^:const id-bigint  (int 44))

(def ^:const id-float   (int 60))
(def ^:const id-double  (int 61))
(def ^:const id-bigdec  (int 62))

(def ^:const id-ratio   (int 70))

;;;; Shared low-level stream stuff

(defn- write-id! [^DataOutputStream stream ^Integer id] (.writeByte stream id))

(defn- write-bytes!
  [^DataOutputStream stream ^bytes ba]
  (let [len (alength ba)]
    (.writeShort stream len) ; Encode size of byte array
    (.write stream ba 0 len)))

(defn- read-bytes!
  ^bytes [^DataInputStream stream]
  (let [len (.readShort stream)
        ba  (byte-array len)]
    (.read stream ba 0 len) ba))

(defn- write-as-bytes!
  "Write arbitrary object as bytes using reflection."
  [^DataOutputStream stream obj]
  (write-bytes! stream (.toByteArray obj)))

(defn- read-biginteger!
  "Wrapper around read-bytes! for common case of reading to a BigInteger.
  Note that as of Clojure 1.3, java.math.BigInteger ≠ clojure.lang.BigInt."
  ^BigInteger [^DataInputStream stream]
  (BigInteger. (read-bytes! stream)))

;;;; Freezing

(defprotocol Freezable (freeze [this stream]))

(comment (meta '^:DataOutputStream s))

(defmacro freezer
  "Helper to extend Freezable protocol."
  [type id & body]
  `(extend-type ~type
     ~'Freezable
     (~'freeze [~'x ~(with-meta 's {:tag 'DataOutputStream})]
       (write-id! ~'s ~id)
       ~@body)))

(freezer (Class/forName "[B") id-bytes (write-bytes! s x))
(freezer nil id-nil)
(freezer Boolean id-boolean (.writeBoolean s x))

(freezer Character            id-char    (.writeChar s (int x)))
(freezer String               id-string  (.writeUTF s x))
(freezer clojure.lang.Keyword id-keyword (.writeUTF s (name x)))

(declare freeze-to-stream!*)

(freezer clojure.lang.IPersistentList id-list
         (.writeInt s (count x)) ; Encode length
         (doseq [i x] (freeze-to-stream!* s i)))
(freezer clojure.lang.IPersistentVector id-vector
         (.writeInt s (count x))
         (doseq [i x] (freeze-to-stream!* s i)))
(freezer clojure.lang.IPersistentMap id-map
         (.writeInt s (count x))
         (doseq [[k v] x]
           (freeze-to-stream!* s k)
           (freeze-to-stream!* s v)))
(freezer clojure.lang.IPersistentSet id-set
         (.writeInt s (count x))
         (doseq [i x] (freeze-to-stream!* s i)))
(freezer clojure.lang.IPersistentCollection id-coll
         (.writeInt s (count x))
         (doseq [i x] (freeze-to-stream!* s i)))

(freezer Byte    id-byte    (.writeByte s x))
(freezer Short   id-short   (.writeShort s x))
(freezer Integer id-integer (.writeInt s x))
(freezer Long    id-long    (.writeLong s x))
(freezer clojure.lang.BigInt  id-bigint (write-as-bytes! s (.toBigInteger x)))
(freezer java.math.BigInteger id-bigint (write-as-bytes! s x))

(freezer Float      id-float  (.writeFloat s x))
(freezer Double     id-double (.writeDouble s x))
(freezer BigDecimal id-bigdec
         (write-as-bytes! s (.unscaledValue x))
         (.writeInt s (.scale x)))

(freezer clojure.lang.Ratio id-ratio
         (write-as-bytes! s (.numerator   x))
         (write-as-bytes! s (.denominator x)))

;; Use Clojure's own reader as final fallback (useful for records, etc.)
(freezer Object id-reader (.writeUTF s (pr-str x)))

(defn- freeze-to-stream!* [^DataOutputStream s x]
  (if-let [m (meta x)]
    (do (write-id! s id-meta)
        (freeze-to-stream!* s m)))
  (freeze x s))

(defn freeze-to-stream!
  "Serializes x to given output stream."
  [data-output-stream x]
  (binding [*print-dup* true] ; For 'pr-str'
    (freeze-to-stream!* data-output-stream x)))

(defn freeze-to-bytes
  "Serializes x to a byte array and returns the array.

  Note that a buffer is used during serialization and its initial size is
  heavily tuned for small data (e.g. single values or small collections)."
  ^bytes [x & {:keys [compress? initial-ba-size]
               :or   {compress? true initial-ba-size 32}}]
  (let [ba     (ByteArrayOutputStream. initial-ba-size)
        stream (DataOutputStream. ba)]
    (freeze-to-stream! stream x)
    (let [ba (.toByteArray ba)]
      (if compress? (Snappy/compress ba) ba))))

;;;; Thawing

(defmacro case-eval
  "Like case but evaluates test constants for their compile-time value."
  [e & clauses]
  (let [;; Don't evaluate default expression!
        default (when (odd? (count clauses)) (last clauses))
        clauses (if default (butlast clauses) clauses)]
    `(case ~e
       ~@(map-indexed (fn [i# form#] (if (even? i#) (eval form#) form#))
                      clauses)
       ~(when default default))))

(defn- thaw-from-stream!*
  [^DataInputStream s]
  (let [type-id (.readByte s)]
    (case-eval
     type-id

     id-reader  (read-string (.readUTF s))
     id-bytes   (read-bytes! s)
     id-nil     nil
     id-boolean (.readBoolean s)

     id-char    (.readChar s)
     id-string  (.readUTF s)
     id-keyword (keyword (.readUTF s))

     id-list   (apply list (repeatedly (.readInt s) (partial thaw-from-stream!* s)))
     id-vector (vec (repeatedly (.readInt s) (partial thaw-from-stream!* s)))
     id-map    (apply hash-map (repeatedly (* 2 (.readInt s))
                                           (partial thaw-from-stream!* s)))
     id-set    (set (repeatedly (.readInt s) (partial thaw-from-stream!* s)))
     id-coll   (doall (repeatedly (.readInt s) (partial thaw-from-stream!* s)))

     id-meta (let [m (thaw-from-stream!* s)] (with-meta (thaw-from-stream!* s) m))

     id-byte    (.readByte s)
     id-short   (.readShort s)
     id-integer (.readInt s)
     id-long    (.readLong s)
     id-bigint  (bigint (read-biginteger! s))

     id-float  (.readFloat s)
     id-double (.readDouble s)
     id-bigdec (BigDecimal. (read-biginteger! s) (.readInt s))

     id-ratio (/ (bigint (read-biginteger! s))
                 (bigint (read-biginteger! s)))

     (throw (Exception. (str "Failed to thaw unknown type ID: " type-id))))))

(defn thaw-from-stream!
  "Deserializes an entity from given input stream. "
  [data-input-stream]
  (binding [*read-eval* false] ; For 'read-string' injection safety - NB!!!
    (thaw-from-stream!* data-input-stream)))

(defn thaw-from-bytes
  "Deserializes an entity from given byte array."
  ([ba] (thaw-from-bytes ba false))
  ([ba compressed?]
     (->> (if compressed? (Snappy/uncompress ba) ba)
          (ByteArrayInputStream.)
          (DataInputStream.)
          (thaw-from-stream!))))

(def stress-data
  {;;:bytes      (byte-array [(byte 1) (byte 2) (byte 3)])
   :nil          nil
   :boolean      true

   :char-utf8    \ಬ
   :string-utf8  "ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ"
   :string-long  (apply str (range 1000))
   :keyword      :keyword

   :list         (list 1 2 3 4 5 (list 6 7 8 (list 9 10)))
   :list-quoted  '(1 2 3 4 5 (6 7 8 (9 10)))
   :list-empty   (list)
   :vector       [1 2 3 4 5 [6 7 8 [9 10]]]
   :vector-empty []
   :map          {:a 1 :b 2 :c 3 :d {:e 4 :f {:g 5 :h 6 :i 7}}}
   :map-empty    {}
   :set          #{1 2 3 4 5 #{6 7 8 #{9 10}}}
   :set-empty    #{}
   :meta         (with-meta {:a :A} {:metakey :metaval})
   :coll         (repeatedly 1000 rand)

   :byte         (byte 16)
   :short        (short 42)
   :integer      (int 3)
   :long         (long 3)
   :bigint       (bigint 31415926535897932384626433832795)

   :float        (float 3.14)
   :double       (double 3.14)
   :bigdec       (bigdec 3.1415926535897932384626433832795)

   :ratio        22/7})

;;;; Dev/tests
(comment
  (defn- roundtrip
    [x & opts]
    (thaw-from-bytes (apply freeze-to-bytes x opts)
                     (:compress? (apply hash-map opts))))
  (defn- reader-roundtrip
    [x] (binding [*print-dup* false ; To allow stress-data forms
                  *read-eval* false]
          (read-string (pr-str x))))
  (defn- good-roundtrip? [roundtrip-fn x] (= x (roundtrip-fn x)))

  (roundtrip stress-data)
  (reader-roundtrip stress-data)
  (good-roundtrip? roundtrip stress-data) ; true
  (good-roundtrip? reader-roundtrip stress-data) ; false (without *print-dup*)

  (time (dotimes [_ 1000] (roundtrip stress-data :compress? false)))
  ;; 950ms after w/u, no compression
  ;; 1000ms after w/u, compression

  (pr-str stress-data)
  (time (dotimes [_ 1000] (reader-roundtrip stress-data)))
  ;; 6200ms after w/u

  (time (dotimes [_ 10000] (roundtrip 12)))        ; 25ms
  (time (dotimes [_ 10000] (reader-roundtrip 12))) ; 50ms
  )