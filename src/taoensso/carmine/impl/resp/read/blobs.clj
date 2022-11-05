(ns taoensso.carmine.impl.resp.read.blobs
  "Blob reading part of RESP3 implementation."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.test    :as test :refer [deftest testing is]]
   [taoensso.encore :as enc  :refer [throws?]]
   [taoensso.nippy  :as nippy]
   [taoensso.carmine.impl.resp.common :as resp-com
    :refer [str->bytes bytes->str xs->in+]]

   [taoensso.carmine.impl.resp.read.common :as read-com])

  (:import
   [java.nio.charset StandardCharsets]
   [java.io DataInputStream]
   [taoensso.carmine.impl.resp.read.common AsThawed]))

(comment
  (remove-ns      'taoensso.carmine.impl.resp.read.blobs)
  (test/run-tests 'taoensso.carmine.impl.resp.read.blobs))

(enc/declare-remote
  taoensso.carmine-v4/issue-83-workaround?)

(alias 'core 'taoensso.carmine-v4)

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
  [read-mode read-markers? ^DataInputStream in]
  (let [size-str (.readLine in)]

    (if-let [stream? (= size-str "?")]
      ;; Streaming
      (read-streaming-blob read-mode in)

      ;; Not streaming
      (let [n (Integer/parseInt size-str)]
        (if (<= n 0) ; Empty or RESP2 null
          (if (== n 0)
            (if (identical? read-mode :bytes) (byte-array 0) "") ; Empty
            nil)

          ;; Not empty
          (if-let [marker
                   (and
                     read-markers?
                     (resp-com/read-blob-?marker in n))]
            ;; Marked
            (read-marked-blob read-mode marker n in)

            ;; Unmarked
            (if (identical? read-mode :skip)

              ;; Skip
              (do
                (.skipBytes            in n)
                (resp-com/discard-crlf in)
                :carmine/_skipped)

              ;; Don't skip
              (let [ba (byte-array n)]
                (.readFully            in ba 0 n)
                (resp-com/discard-crlf in)
                (complete-blob read-mode ba)))))))))

(let [discard-stream-separator resp-com/discard-stream-separator
      discard-crlf             resp-com/discard-crlf]

  (defn- read-streaming-blob
    [read-mode ^DataInputStream in]

    (if (identical? read-mode :skip)

      ;; Skip
      (loop []
        (discard-stream-separator in)
        (let [n (Integer/parseInt (.readLine in))]
          (if (== n 0)
            :carmine/_skipped ; Stream complete

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
            (if (== n 0)

              ;; Stream complete
              (complete-blob read-mode (.toByteArray baos))

              ;; Stream continues
              (let [ba (byte-array n)]
                (.readFully   in ba 0 n)
                (discard-crlf in)
                (.write baos ba 0 (alength ba))
                (recur)))))))))

(defn- read-marked-blob
  [read-mode marker marked-size ^DataInputStream in]
  (let [n (- ^int marked-size 2)
        ?ba
        (when (pos? n)
          (let [ba (byte-array  n)]
            (.readFully in ba 0 n)
            (do            ba)))]

    (resp-com/discard-crlf in)
    (case marker
      :nil nil
      :bin (or ?ba (byte-array 0))
      :npy
      (let [?thaw-opts (read-com/read-mode->?thaw-opts read-mode)]
        ;; ?ba should be nnil when marked
        (blob->thawed ?thaw-opts ?ba)))))

;;;; Read-mode handling

(defn- blob->thawed [?thaw-opts ba]
  (try
    (nippy/thaw ba ?thaw-opts)
    (catch Throwable t
      (resp-com/reply-error
        (ex-info "[Carmine] Nippy threw an error while thawing blob reply"
          (enc/assoc-when
            {:eid :carmine.resp.read.blob/nippy-thaw-error
             :thaw-opts ?thaw-opts
             :bytes {:length (count ba) :content ba}}
            :possible-non-nippy-bytes? core/issue-83-workaround?)
          t)))))

(defn- complete-blob [read-mode ba]
  (enc/cond!
    (identical?    read-mode    nil) (bytes->str ba) ; Common case
    (identical?    read-mode :bytes)             ba
    ;; (identical? read-mode :skip) :carmine/_skipped ; Shouldn't be here at all
    :if-let [thaw-opts (read-com/read-mode->?thaw-opts read-mode)]
    (blob->thawed thaw-opts ba)))

;;;; Tests

(defn- empty-bytes? [ba] (enc/ba= ba (byte-array 0)))

(deftest ^:private _read-blob
  [(testing "Basics"
     [(is (= ""         (read-blob nil    true (xs->in+  0))) "As default: empty blob")
      (is (empty-bytes? (read-blob :bytes true (xs->in+  0))) "As bytes:   empty blob")

      (is (= nil (read-blob nil    true (xs->in+ -1))) "As default: RESP2 null")
      (is (= nil (read-blob :bytes true (xs->in+ -1))) "As bytes:   RESP2 null")

      (is (=             (read-blob nil    true (xs->in+ 5 "hello"))  "hello"))
      (is (= (bytes->str (read-blob :bytes true (xs->in+ 5 "hello"))) "hello"))

      (is (=             (read-blob nil    true (xs->in+ 7 "hello\r\n"))  "hello\r\n") "Binary safe")
      (is (= (bytes->str (read-blob :bytes true (xs->in+ 7 "hello\r\n"))) "hello\r\n") "Binary safe")

      (let [pattern {:eid :carmine.resp.read/missing-crlf}]
        [(is (throws? :common pattern (read-blob nil    true (resp-com/str->in "5\r\nhello"))))
         (is (throws? :common pattern (read-blob :bytes true (resp-com/str->in "5\r\nhello"))))
         (is (throws? :common pattern (read-blob nil    true (resp-com/str->in "5\r\nhello__"))))
         (is (throws? :common pattern (read-blob :bytes true (resp-com/str->in "5\r\nhello__"))))])])

   (testing "Streaming"
     [(is (=             (read-blob nil    true (xs->in+ "?" ";5" "hello" ";1" " " ";6" "world!" ";0"))  "hello world!"))
      (is (= (bytes->str (read-blob :bytes true (xs->in+ "?" ";5" "hello" ";1" " " ";6" "world!" ";0"))) "hello world!"))

      (let [pattern {:eid :carmine.resp.read/missing-stream-separator}]
        [(is (throws? :common pattern (read-blob nil    true (xs->in+ "?" ";5" "hello" "1" " " ";6" "world!" ";0"))))
         (is (throws? :common pattern (read-blob :bytes true (xs->in+ "?" ";5" "hello" "1" " " ";6" "world!" ";0"))))])])

   (testing "Marked blobs"
     ;; See also `common/_read-blob-?marker` tests
     [(is (=             (read-blob nil true  (xs->in+ 5 "\u0000more"))   "\u0000more"))
      (is (=             (read-blob nil true  (xs->in+ 2 "\u0000_"))               nil))
      (is (=             (read-blob nil false (xs->in+ 2 "\u0000_"))         "\u0000_"))
      (is (= (bytes->str (read-blob nil true  (xs->in+ 6 "\u0000<more")))       "more"))
      (is (=             (read-blob nil false (xs->in+ 6 "\u0000<more")) "\u0000<more"))

      (let [data       nippy/stress-data-comparable
            ba         (nippy/freeze data)
            marked-ba  (resp-com/xs->ba resp-com/ba-npy ba)
            marked-len (alength ^bytes marked-ba)]

        (is (= (read-blob nil true (xs->in+ marked-len marked-ba)) data) "Simple Nippy data"))

      (let [data       nippy/stress-data-comparable
            pwd        [:salted "secret"]
            ba         (nippy/freeze data {:password pwd})
            marked-ba  (resp-com/xs->ba resp-com/ba-npy ba)
            marked-len (alength ^bytes marked-ba)]

        [(is (= (read-blob (AsThawed. {:password pwd}) true (xs->in+ marked-len marked-ba)) data)
           "Encrypted Nippy data (good password)")

         (let [r (read-blob nil true (xs->in+ marked-len marked-ba))]

           [(is (resp-com/reply-error? {:eid :carmine.resp.read.blob/nippy-thaw-error} r)
              "Encrypted Nippy data (bad password)")

            (is (enc/ba= (-> r resp-com/get-reply-error ex-data :bytes :content) ba)
              "Unthawed Nippy data still provided")])])])])
