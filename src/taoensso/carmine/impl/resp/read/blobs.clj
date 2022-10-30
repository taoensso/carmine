(ns taoensso.carmine.impl.resp.read.blobs
  "Blob reading part of RESP3 implementation."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.string  :as str]
   [clojure.test    :as test :refer [deftest testing is]]
   [taoensso.encore :as enc  :refer [throws?]]
   [taoensso.nippy  :as nippy]
   [taoensso.carmine.impl.resp.common :as com
    :refer [str->bytes bytes->str xs->in+]])

  (:import
   [java.nio.charset StandardCharsets]
   [java.io DataInputStream]))

(comment
  (remove-ns      'taoensso.carmine.impl.resp.read.blobs)
  (test/run-tests 'taoensso.carmine.impl.resp.read.blobs))

;;;; Blob xforms

(def ^:dynamic *bxf*
  "Active ?BXF (blob xform), implementation detail!
  Will influence only null and user-blob replies."
  nil)

(deftype BXF [kind more]) ; Mutually-exclusive kind e/o #{:skip :bytes :ba-fn :str-fn :thaw}

(let [bytes->str bytes->str]
  (def bxf-skip    (BXF. :skip  nil)) ; Used as identity
  (def bxf-bytes   (BXF. :bytes nil)) ; Used as identity
  (def bxf-long    (BXF. :str-fn (fn [?s] (enc/as-int    ?s))))
  (def bxf-?long   (BXF. :str-fn (fn [?s] (enc/as-?int   ?s))))
  (def bxf-double  (BXF. :str-fn (fn [?s] (enc/as-float  ?s))))
  (def bxf-?double (BXF. :str-fn (fn [?s] (enc/as-?float ?s))))
  (def bxf-kw      (BXF. :str-fn (fn [?s] (enc/as-kw     ?s))))
  (def bxf-?kw     (BXF. :str-fn (fn [?s] (enc/as-?kw    ?s)))))

(do
  (defmacro as-default [& body] `(binding [*bxf* nil]         (do ~@body)))
  (defmacro as-skipped [& body] `(binding [*bxf* bxf-skip]    (do ~@body)))
  (defmacro as-bytes   [& body] `(binding [*bxf* bxf-bytes]   (do ~@body)))

  (defmacro as-long    [& body] `(binding [*bxf* bxf-long]    (do ~@body)))
  (defmacro as-?long   [& body] `(binding [*bxf* bxf-?long]   (do ~@body)))

  (defmacro as-double  [& body] `(binding [*bxf* bxf-double]  (do ~@body)))
  (defmacro as-?double [& body] `(binding [*bxf* bxf-?double] (do ~@body)))

  (defmacro as-kw      [& body] `(binding [*bxf* bxf-kw]      (do ~@body)))
  (defmacro as-?kw     [& body] `(binding [*bxf* bxf-?kw]     (do ~@body)))

  (defmacro as-parsed-bytes [ba-fn     & body] `(binding [*bxf* (BXF. :ba-fn    ~ba-fn)] (do ~@body)))
  (defmacro as-parsed-str   [str-fn    & body] `(binding [*bxf* (BXF. :str-fn  ~str-fn)] (do ~@body)))
  (defmacro as-thawed       [thaw-opts & body] `(binding [*bxf* (BXF. :thaw ~thaw-opts)] (do ~@body))))

;;;; High-level reader

(declare
  ^:private read-streaming-blob
  ^:private read-marked-blob

  ^:private blob->thawed
  ^:private blob->parsed-as-?bytes
  ^:private blob->parsed-as-?str
  ^:private complete-blob)

(defn read-blob
  "$<length>\r\n<bytes>\r\n -> ?<binary safe String or other>"
  [user-blob? ^DataInputStream in]
  (let [size-str (.readLine in)
        ?bxf     (when user-blob? *bxf*)]

    (if-let [stream? (= size-str "?")]
      ;; Streaming
      (read-streaming-blob ?bxf in)

      ;; Not streaming
      (let [n (Integer/parseInt size-str)]
        (if (<= n 0)
          (complete-blob ?bxf nil (== n -1)) ; Empty or RESP2 null

          ;; Not empty
          (if-let [marker
                   (and
                     user-blob?
                     com/read-blob-markers?
                     (com/read-blob-?marker in n))]
            ;; Marked
            (read-marked-blob ?bxf marker n in)

            ;; Unmarked
            (if (identical? ?bxf bxf-skip)

              ;; Skip
              (do
                (.skipBytes       in n)
                (com/discard-crlf in)
                :carmine.read/skipped)

              ;; Don't skip
              (let [ba (byte-array n)]
                (.readFully       in ba 0 n)
                (com/discard-crlf in)
                (complete-blob     ?bxf ba false)))))))))

(let [discard-stream-separator com/discard-stream-separator
      discard-crlf             com/discard-crlf]

  (defn- read-streaming-blob
    [?bxf ^DataInputStream in]

    (if (identical? ?bxf bxf-skip)

      ;; Skip
      (loop []
        (discard-stream-separator in)
        (let [n (Integer/parseInt (.readLine in))]
          (if (zero? n)
            :carmine.read/skipped ; Stream complete

            ;; Stream continues
            (do
              (.skipBytes   in n)
              (discard-crlf in)
              (recur)))))

      ;; Don't skip
      ;; Note: even if the final output is a String, it's faster
      ;; to accumulate to BAOS then transform to a String at the
      ;; end rather than use a StringBuffer.
      (let [baos (java.io.ByteArrayOutputStream. 128)]
        (loop []
          (discard-stream-separator in)
          (let [n (Integer/parseInt (.readLine in))]
            (if (zero? n)

              ;; Stream complete
              (complete-blob ?bxf (.toByteArray baos) false)

              ;; Stream continues
              (let [ba (byte-array n)]
                (.readFully   in ba 0 n)
                (discard-crlf in)
                (.write baos ba 0 (alength ba))
                (recur)))))))))

(defn- read-marked-blob
  [?bxf marker marked-size ^DataInputStream in]
  (let [n (- ^int marked-size 2)
        ?ba
        (when (pos? n)
          (let [ba (byte-array  n)]
            (.readFully in ba 0 n)
            (do            ba)))]

    (com/discard-crlf in)
    (case marker
      :nil nil
      :bin (if (nil? ?ba) (byte-array 0) ?ba)
      :npy
      (let [?thaw-opts
            (when-let [^BXF bxf ?bxf]
              (when (identical? (.-kind bxf) :thaw)
                (.-more bxf)))]
        ;; Assume ?ba nnil when marked
        (blob->thawed ?thaw-opts ?ba)))))

;;;; BXF handling

(defn- blob->thawed [?thaw-opts ba]
  (try
    (nippy/thaw ba ?thaw-opts)
    (catch Throwable t
      (com/carmine-reply-error
        (ex-info "[Carmine] Nippy threw an error while thawing blob reply"
          (enc/assoc-when
            {:eid :carmine.resp.read.blob/nippy-thaw-error
             :thaw-opts ?thaw-opts
             :bytes {:length (count ba) :content ba}}
            :possible-non-nippy-bytes? com/issue-83-workaround?)
          t)))))

(defn- blob->parsed-as-?bytes [ba-fn ?ba]
  (try
    (ba-fn ?ba)
    (catch Throwable t
      (com/carmine-reply-error
        (ex-info "[Carmine] Active parse fn threw an error while parsing blob reply (as bytes)"
          {:eid :carmine.resp.read.blob/parse-fn-error
           :parse-as :bytes
           :parse-fn ba-fn
           :bytes {:length (count ?ba) :content ?ba}}
          t)))))

(defn- blob->parsed-as-?str [str-fn ?ba]
  (let [?s
        (when-let [ba ?ba]
          (try
            (bytes->str ba)
            (catch Throwable t
              (com/carmine-reply-error
                (ex-info "[Carmine] Error converting blob reply from bytes to string for active parser"
                  {:eid :carmine.resp.read.blob/parse-fn-ba->str-error
                   :parse-as :string
                   :parse-fn str-fn
                   :bytes {:length (count ?ba) :content ?ba}}
                  t)))))]

    (try
      (str-fn ?s)
      (catch Throwable t
        (com/carmine-reply-error
          (ex-info "[Carmine] Active parse fn threw an error while parsing blob reply (as string)"
            {:eid :carmine.resp.read.blob/parse-fn-error
             :parse-as :string
             :parse-fn str-fn
             :bytes    {:length (count ?ba) :content ?ba}
             :string   ?s}
            t))))))

(defn complete-blob
  "Intended behaviour:
    - `as-default` on nil   reply => nil,
                   on empty reply => \"\"

    - `as-thaw`    on nil/empty reply => nil
    - `as-bytes`   on nil/empty reply => (byte-array 0)
    - `as-parsed`  on nil/empty reply => (parser-fn nil)"

  [?bxf ?ba null?]
  (if (nil? ?bxf) ; Common case
    (if (nil? ?ba)
      (if null? nil "")
      (bytes->str ?ba))

    (let [^BXF bxf ?bxf
          kind (.-kind bxf)]

      (case kind
        ;; :skip nil ; Shouldn't be here for :skip kinds
        :bytes  (if (nil? ?ba) (byte-array 0)                             ?ba)
        :ba-fn  (let [ba-fn  (.-more bxf)] (blob->parsed-as-?bytes ba-fn  ?ba))
        :str-fn (let [str-fn (.-more bxf)] (blob->parsed-as-?str   str-fn ?ba))
        :thaw
        (if (nil? ?ba)
          nil
          (let [thaw-opts (.-more bxf)]
            (blob->thawed thaw-opts ?ba)))

        (throw
          (ex-info "[Carmine] Unknown active blob xform (bxf) kind while processing blob reply"
            {:eid :carmine.resp.read.blob/unknown-bxf-kind
             :bxf   {:kind kind :kind-type (type kind)}
             :bytes {:length (count ?ba) :content ?ba
                     :kind (if null? :null (if (nil? ?ba) :empty :non-empty))}}))))))

;;;; Tests

(deftest ^:private _read-blob
  [(testing "Basics"
     [(is (=                (read-blob true (xs->in+  0))  "")  "Empty string")
      (is (= (vec (as-bytes (read-blob true (xs->in+  0)))) []) "Empty bytes")

      (is (=                (read-blob true (xs->in+ -1))  nil) "RESP2 null")
      (is (= (vec (as-bytes (read-blob true (xs->in+ -1)))) []) "RESP2 null")

      (is (=                       (read-blob true (xs->in+ 5 "hello"))   "hello"))
      (is (= (bytes->str (as-bytes (read-blob true (xs->in+ 5 "hello")))) "hello"))

      (is (=                       (read-blob true (xs->in+ 7 "hello\r\n"))  "hello\r\n")  "Binary safe")
      (is (= (bytes->str (as-bytes (read-blob true (xs->in+ 7 "hello\r\n")))) "hello\r\n") "Binary safe")

      (let [pattern {:eid :carmine.resp.read/missing-crlf}]
        [(is (throws? :common pattern           (read-blob true             (com/str->in "5\r\nhello"))))
         (is (throws? :common pattern (as-bytes (read-blob true (com/str->in "5\r\nhello")))))
         (is (throws? :common pattern           (read-blob true             (com/str->in "5\r\nhello__"))))
         (is (throws? :common pattern (as-bytes (read-blob true (com/str->in "5\r\nhello__")))))])])

   (testing "Streaming"
     [(is (=                       (read-blob true (xs->in+ "?" ";5" "hello" ";1" " " ";6" "world!" ";0"))   "hello world!"))
      (is (= (bytes->str (as-bytes (read-blob true (xs->in+ "?" ";5" "hello" ";1" " " ";6" "world!" ";0")))) "hello world!"))

      (let [pattern {:eid :carmine.resp.read/missing-stream-separator}]
        [(is (throws? :common pattern           (read-blob true (xs->in+ "?" ";5" "hello" "1" " " ";6" "world!" ";0"))))
         (is (throws? :common pattern (as-bytes (read-blob true (xs->in+ "?" ";5" "hello" "1" " " ";6" "world!" ";0")))))])])

   (testing "Marked blobs"
     ;; See also `common/_read-blob-?marker` tests
     [(is (=             (read-blob true  (xs->in+ 5 "\u0000more"))   "\u0000more"))
      (is (=             (read-blob true  (xs->in+ 2 "\u0000_"))               nil))
      (is (=             (read-blob false (xs->in+ 2 "\u0000_"))         "\u0000_"))
      (is (= (bytes->str (read-blob true  (xs->in+ 6 "\u0000<more")))       "more"))
      (is (=             (read-blob false (xs->in+ 6 "\u0000<more")) "\u0000<more"))

      (let [data       nippy/stress-data-comparable
            ba         (nippy/freeze data)
            marked-ba  (com/xs->ba com/ba-npy ba)
            marked-len (alength ^bytes marked-ba)]

        (is (= (read-blob true (xs->in+ marked-len marked-ba)) data) "Simple Nippy data"))

      (let [data       nippy/stress-data-comparable
            pwd        [:salted "secret"]
            ba         (nippy/freeze data {:password pwd})
            marked-ba  (com/xs->ba com/ba-npy ba)
            marked-len (alength ^bytes marked-ba)]

        [(is (= (as-thawed {:password pwd} (read-blob true (xs->in+ marked-len marked-ba))) data)
           "Encrypted Nippy data (good password)")

         (let [r (read-blob true (xs->in+ marked-len marked-ba))]

           [(is (com/crex-match? r {:eid :carmine.resp.read.blob/nippy-thaw-error})
              "Encrypted Nippy data (bad password)")

            (is (enc/ba= (-> r com/get-carmine-reply-error ex-data :bytes :content) ba)
              "Unthawed Nippy data still provided")])])])])

(deftest ^:private _*bxf*
  [(testing "Basics"
     [(is (=            (read-blob true (xs->in+ 1 8)) "8"))
      (is (= (as-long   (read-blob true (xs->in+ 1 8))) 8))
      (is (= (as-double (read-blob true (xs->in+ 1 8))) 8.0))

      (is (=                       (read-blob true (xs->in+ 5 "hello"))   "hello"))
      (is (=             (as-kw    (read-blob true (xs->in+ 5 "hello")))  :hello))
      (is (= (bytes->str (as-bytes (read-blob true (xs->in+ 5 "hello")))) "hello"))
      (is (=
            (as-parsed-str str/upper-case
              (read-blob true (xs->in+ 5 "hello"))) "HELLO"))

      (is (= (bytes->str           (read-blob true (xs->in+ "5" (com/xs->ba com/ba-bin [\a \b \c]))))  "abc"))
      (is (= (bytes->str (as-bytes (read-blob true (xs->in+ "5" (com/xs->ba com/ba-bin [\a \b \c]))))) "abc")
        "Mark ba still removed from returned bytes, even with `as-bytes`.")])

   (testing "Null/empty blobs"
     [(is (= (read-blob true (xs->in+ -1)) nil) "as-default vs nil")
      (is (= (read-blob true (xs->in+  0))  "") "as-default vs empty")

      (is (= (as-thawed {} (read-blob true (xs->in+ -1))) nil) "as-thawed vs nil")
      (is (= (as-thawed {} (read-blob true (xs->in+  0))) nil) "as-thawed vs empty")

      (is (= (vec (as-bytes {} (read-blob true (xs->in+ -1)))) []) "as-bytes vs nil")
      (is (= (vec (as-bytes {} (read-blob true (xs->in+  0)))) []) "as-bytes vs empty")

      (is (= (as-?long (read-blob true (xs->in+ -1))) nil) "as-?long vs nil")
      (is (= (as-?long (read-blob true (xs->in+  0))) nil) "as-?long vs empty")

      (let [pattern {:eid :carmine.resp.read.blob/parse-fn-error}]
        [(is (com/crex-match? (as-long (read-blob true (xs->in+ -1))) pattern) "as-?long vs nil")
         (is (com/crex-match? (as-long (read-blob true (xs->in+  0))) pattern) "as-?long vs empty")])

      (let [str-fn #(when % (str/upper-case %))]
        [(is (= (as-parsed-str str-fn (read-blob true (xs->in+ -1))) nil) "as-parsed-str vs nil")
         (is (= (as-parsed-str str-fn (read-blob true (xs->in+  0))) nil) "as-parsed-str vs empty")])

      (let [ba-fn #(when % (str/upper-case (bytes->str %)))]
        [(is (= (as-parsed-bytes ba-fn (read-blob true (xs->in+ -1))) nil) "as-parsed-bytes vs nil")
         (is (= (as-parsed-bytes ba-fn (read-blob true (xs->in+  0))) nil) "as-parsed-bytes vs empty")])

      (is (= (vec (read-blob true (xs->in+ 2 (com/xs->ba com/ba-bin [])))) []) "Marked ba-bin vs empty bytes")

      (is (com/crex-match? (read-blob true (xs->in+ 2 (com/xs->ba com/ba-npy [])))
            {:eid :carmine.resp.read.blob/nippy-thaw-error})
        "Marked ba-npy vs empty bytes")])])
