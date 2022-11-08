(ns taoensso.carmine.impl.resp.common
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.test    :as test :refer [deftest testing is]]
   [taoensso.encore :as enc  :refer [throws?]])

  (:import
   [java.nio.charset StandardCharsets]
   [java.io DataInputStream]))

(comment
  (remove-ns      'taoensso.carmine.impl.resp.common)
  (test/run-tests 'taoensso.carmine.impl.resp.common))

(enc/declare-remote
  ^:dynamic taoensso.carmine-v4/*auto-deserialize?*
  ^:dynamic taoensso.carmine-v4/*keywordize-maps?*
            taoensso.carmine-v4/issue-83-workaround?)

(alias 'core 'taoensso.carmine-v4)

;;;;

(def unicode-string "ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ\r\n")
(defn bytes->str ^String [^bytes ba] (String.   ba StandardCharsets/UTF_8))
(defn str->bytes  ^bytes [^String s] (.getBytes s  StandardCharsets/UTF_8))

(deftest ^:private _byte-strings
  [(is (= (bytes->str (str->bytes unicode-string)) unicode-string)
     "UTF_8 charset for byte strings")])

(def ba-crlf (str->bytes "\r\n"))

;;;;

(defn ba->in ^DataInputStream [^bytes ba]
  (-> ba
    java.io.ByteArrayInputStream.
    java.io.BufferedInputStream.
    DataInputStream.))

(defn str->in ^DataInputStream [^String s]
  (ba->in (.getBytes s StandardCharsets/UTF_8)))

(defmacro with-out
  "Body -> bytes"
  [& body]
  `(let [baos# (java.io.ByteArrayOutputStream.)
         ~'out (java.io.BufferedOutputStream. baos#)]
     (do ~@body)
     (.flush       ~'out)
     (.toByteArray baos#)))

(defmacro           with-out->str [& body] `(bytes->str (with-out ~@body)))
(deftest ^:private _with-out->str
  [(is (= (with-out->str (.write out (str->bytes unicode-string))) unicode-string))])

(defmacro           with-out->in [& body] `(ba->in (with-out ~@body)))
(deftest ^:private _with-out->in
  [(is (= (.readLine (with-out->in (.write out (str->bytes "hello\r\n")))) "hello"))])

(defn xseq->ba ^bytes [with-crlfs? xseq]
  (with-out
    (doseq [x xseq]
      (enc/cond!
        (enc/bytes? x) (.write out                 ^bytes x)
        (string?    x) (.write out       (str->bytes      x))
        (int?       x) (.write out       (str->bytes (str x)))
        (char?      x) (.write out                   (int x))
        (vector?    x) (.write out (byte-array (mapv byte x))))

      (when with-crlfs?
        (.write out ^bytes ba-crlf)))))

(do ; Variations useful for tests, etc.
  (defn xs->in+ ^DataInputStream [& xs] (ba->in (xseq->ba true  xs)))
  (defn xs->in  ^DataInputStream [& xs] (ba->in (xseq->ba false xs)))
  (defn xs->ba+           ^bytes [& xs]         (xseq->ba true  xs))
  (defn xs->ba            ^bytes [& xs]         (xseq->ba false xs)))

(deftest ^:private _xs->ba
  [(is (= (bytes->str (xs->ba  "a" "b" 1 (byte-array [(byte \A) (byte \B)]) \C [\d \e])) "ab1ABCde"))
   (is (= (bytes->str (xs->ba+ "a" "b" 1 (byte-array [(byte \A) (byte \B)]) \C [\d \e])) "a\r\nb\r\n1\r\nAB\r\nC\r\nde\r\n"))])

;;;;

(defn skip1 ^DataInputStream [^java.io.DataInput in] (.skipBytes in 1) in)

(deftest ^:private _skip1
  [(is (= (.readLine (skip1 (with-out->in (.write out (str->bytes "+hello\r\n"))))) "hello"))])

;;;; Blob markers

(do
  (def ba-npy (str->bytes "\u0000>"))
  (def ba-bin (str->bytes "\u0000<"))
  (def ba-nil (str->bytes "\u0000_")))

(defn read-blob-?marker
  "Returns e/o {nil :nil :bin :npy}, and possibly advances position
  in stream to skip (consume) any blob markers (`ba-npy`, etc.)."
  [^DataInputStream in ^long n]
  ;; Won't be called if `*auto-deserialize?*` is false
  (when (>= n 2) ; >= 2 for marker+?payload
    (.mark in 2)
    (if-not (== (.readByte in) 0) ; Possible marker iff 1st byte null
      (do (.reset in) nil)
      (enc/case-eval (.readByte in) ; 2nd byte would identify marker kind
        (byte \_) :nil ; ba-nil
        (byte \>) :npy ; ba-npy
        (byte \<)      ; ba-bin
        (enc/cond
          (not core/issue-83-workaround?) :bin
          (< n 7)                         :bin ; >= +5 for Nippy payload (4 header + data)
          :do (.mark in 3)
          (not (== (.readByte in) #=(byte \N))) (do (.reset in) :bin)
          (not (== (.readByte in) #=(byte \P))) (do (.reset in) :bin)
          (not (== (.readByte in) #=(byte \Y))) (do (.reset in) :bin)
          :else                                 (do (.reset in) :npy))

        (do (.reset in) nil)))))

(defn- test-blob-?marker [s]
  (let [^bytes ba (str->bytes s)
        in (ba->in ba)]
    [(read-blob-?marker in (alength ba))
     (.readLine         in)]))

(deftest ^:private _read-blob-?marker
  [(is (= (test-blob-?marker "foo")            [nil "foo"]))
   (is (= (test-blob-?marker "\u0000more")     [nil "\u0000more"]))
   (is (= (test-blob-?marker "\u0000_more")    [:nil "more"]))
   (is (= (test-blob-?marker "\u0000>more")    [:npy "more"]))
   (is (= (test-blob-?marker "\u0000<more")    [:bin "more"]))
   (is (= (test-blob-?marker "\u0000<NPYmore") [:npy "NPYmore"]))
   (is (= (test-blob-?marker "\u0000<NPmore")  [:bin "NPmore"]))
   (is (= (test-blob-?marker "\u0000<Nmore")   [:bin "Nmore"]))])

;; TODO Util for users to parse-?marked-ba -> [<kind> <payload>]

;;;; Errors

(defn throw! [x] (throw (ex-info "Simulated throw" {:arg {:value x :type (type x)}})))

(deftype      ReplyError [ex-info])
(defn        reply-error [ex-info] (ReplyError.  ex-info))
(defn    get-reply-error [x] (when (instance? ReplyError x)        (.-ex-info ^ReplyError x)))
(defn throw-?reply-error [x] (when (instance? ReplyError x) (throw (.-ex-info ^ReplyError x))))
(defn        reply-error?
  ([            x] (instance? ReplyError x))
  ([ex-data-sub x]
   (when-let [e (get-reply-error x)]
     (enc/submap? (ex-data e) ex-data-sub))))

;;;;

(let [ref-b (byte \;)]
  (defn discard-stream-separator
    [^DataInputStream in]
    ;; (.skip 1)
    (let [read-b (.readByte in)] ; -1 if nothing to read
      (if (== ref-b read-b)
        true
        (throw
          (ex-info "[Carmine] Missing stream separator"
            {:eid :carmine.read/missing-stream-separator
             :read {:as-byte read-b :as-char (char read-b)}}))))))

(comment :see-tests-below)

(defn discard-crlf
  [^DataInputStream in]
  ;; (.skip 2)
  (let [s (.readLine in)] ; nil if nothing to read
    (if (= s "")
      true
      (throw
        (ex-info "[Carmine] Missing CRLF"
          {:eid :carmine.read/missing-crlf
           :read s})))))

(deftest ^:private _utils
  [(is (->> (discard-stream-separator (xs->in+ "")) (throws? :common {:eid :carmine.read/missing-stream-separator})))
   (is (true? (discard-crlf (xs->in+ ""))))
   (is (->>   (discard-crlf (xs->in+ "_")) (throws? :common {:eid :carmine.read/missing-crlf})))])
