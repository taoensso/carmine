(ns taoensso.carmine.impl.resp.common
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.string  :as str]
   [clojure.test    :as test :refer [deftest testing is]]
   [taoensso.encore :as enc  :refer [throws?]])

  (:import
   [java.nio.charset StandardCharsets]
   [java.io DataInputStream]))

(comment
  (remove-ns      'taoensso.carmine.impl.resp.common)
  (test/run-tests 'taoensso.carmine.impl.resp.common))

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

;;;; Config

(do
  (def ^:const target-version
    "A high-level option to specify the \"target\" version of Carmine.
    A best effort will be made to try maintain compatibility with
    that version when possible.

    Intended to help ease migration to newer Carmine versions.
    See the relevant CHANGELOG for details.

    As of the latest major version of Carmine (v4), valid options are:

      4 - Disable all available backwards-compatibility features (target Carmine v4).
      3 - Enable  all available backwards-compatibility features for Carmine v3.

    When nil (default):
      - Enable  features relating to data backwards-compatibility.
      - Disable features relating to API  backwards-compatibility."

    (enc/as-?int
      (enc/get-sys-val
        "taoensso.carmine.target-version"
        "TAOENSSO_CARMINE_TARGET_VERSION")))

  (def ^:const read-blob-markers?
    "Auto deserialize non-(string, keyword, number) types?"
    (enc/get-sys-bool
      (case target-version (1 2 3 nil) true 4 false)
      "taoensso.carmine.legacy.read-blob-markers"
      "TAOENSSO_CARMINE_LEGACY_READ_BLOB_MARKERS"))

  (def ^:const write-blob-markers?
    "Auto serialize non-(string, keyword, number) types?"
    (enc/get-sys-bool
      ;; (case target-version (1 2 3 #_nil) true (4 nil) false) ; TODO BREAKING
      (case    target-version (1 2 3   nil) true  4      false)
      "taoensso.carmine.legacy.write-blob-markers"
      "TAOENSSO_CARMINE_LEGACY_WRITE_BLOB_MARKERS"))

  (def ^:const reserve-nulls?
    (enc/get-sys-bool true
      "taoensso.carmine.legacy.reserve-nulls"
      "TAOENSSO_CARMINE_LEGACY_RESERVE_NULLS"))

  (def ^:const issue-83-workaround?
    "A bug in v2.6.0 (2014-04-01) to v2.6.1 (2014-05-01) caused Nippy
    blobs to be marked with `ba-bin` instead of `ba-npy`,
    Ref. https://github.com/ptaoussanis/carmine/issues/83

    Only relevant if `read-blob-markers?` is true."
    (enc/get-sys-bool
      ;; (case target-version (1 2 3 #_nil) true (4 nil) false) ; TODO BREAKING
      (case target-version    (1 2 3   nil) true  4      false)
      "taoensso.carmine.issue-83-workaround"
      "TAOENSSO_CARMINE_ISSUE_83_WORKAROUND")))

;;;; Consts, etc.

(do ; Blob markers
  (def ba-npy (str->bytes "\u0000>"))
  (def ba-bin (str->bytes "\u0000<"))
  (def ba-nil (str->bytes "\u0000_")))

(defn read-blob-?marker
  "Returns e/o {nil :nil :bin :npy}, and possibly advances position
    in stream to skip (consume) any blob markers (`ba-npy`, etc.)."
  [^DataInputStream in ^long n]
  ;; Won't be called if `read-blob-markers?` is false
  (when (>= n 2) ; >= 2 for marker+?payload
    (.mark in 2)
    (if-not (zero? (.readByte in)) ; Possible marker iff 1st byte null
      (do (.reset in) nil)
      (enc/case-eval (.readByte in) ; 2nd byte would identify marker kind
        (byte \_) :nil ; ba-nil
        (byte \>) :npy ; ba-npy
        (byte \<)      ; ba-bin
        (enc/cond
          (not issue-83-workaround?) :bin
          (< n 7)                    :bin ; >= +5 for Nippy payload (4 header + data)
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

;;;;

;; Wrapper to identify Carmine-generated reply errors for
;; possible throwing or unwrapping
(deftype  CarmineReplyError [ex-info])
(defn     carmine-reply-error [x] (CarmineReplyError. x))
(defn get-carmine-reply-error [x]
  (when (instance? CarmineReplyError x)
    (.-ex-info ^CarmineReplyError x)))

(defn crex-match? [x ex-data-sub]
  (enc/submap? (ex-data (get-carmine-reply-error x))
    ex-data-sub))

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
            {:eid :carmine.resp.read/missing-stream-separator
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
          {:eid :carmine.resp.read/missing-crlf
           :read s})))))

(deftest ^:private _utils
  [(throws? :common {:eid :carmine.resp.read/missing-stream-separator}
     (discard-stream-separator (xs->in+ "")))

   (is (true? (discard-crlf (xs->in+ ""))))
   (is (throws? :common {:eid :carmine.resp.read/missing-crlf}
         (discard-crlf (xs->in+ "_"))))])
