(ns taoensso.carmine-v4
  "Experimental, baggage-free modern Redis client prototype."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.test     :as test :refer [deftest testing is]]
   [taoensso.encore  :as enc  :refer [throws?]]
   [taoensso.carmine :as legacy-core]
   [taoensso.carmine
    [connections :as legacy-conns]
    [protocol    :as legacy-protocol]
    [commands    :as legacy-cmds]]

   [taoensso.carmine.impl.resp.common       :as resp-com]
   [taoensso.carmine.impl.resp.read.common  :as read-com]
   [taoensso.carmine.impl.resp.read         :as read]
   [taoensso.carmine.impl.resp.write        :as write]
   [taoensso.carmine.impl.resp.read.blobs   :as blobs]
   [taoensso.carmine.impl.resp.read.parsing :as parsing]
   [taoensso.carmine.impl.resp              :as resp])

  (:refer-clojure :exclude [parse-long parse-double]))

(comment
  (remove-ns      'taoensso.carmine-v4)
  (test/run-tests 'taoensso.carmine-v4)
  (run-all-carmine-tests))

(defn run-all-carmine-tests []
  (merge-with (fn [l r] (if (keyword? r) r (+ l r)))
    (test/run-all-tests #"taoensso\.carmine\.impl\..*")
    (test/run-tests      'taoensso.carmine-v4)
    (test/run-tests      'taoensso.carmine.tests.v4)))

(enc/assert-min-encore-version [3 32 0])

;;;; TODO

;; - Add common and v4 util to parse-?marked-ba -> [<kind> <payload>]
;; - Add dummy (local?) replies
;; - Move *push-fn* to v4, finish implementation, document
;; - v4: add freeze & thaw stuff (incl. bindings)

;; - Investigate Sentinel &/or Cluster
;; - Investigate new conns design (incl. breakage tests, etc.)
;;   - (wcar {:hello {}}) and/or (wcar {:init-fn _}) support

;; - Refactor connections API
;; - Refactor commands, add modules support
;; - Refactor stashing :as-pipeline, etc.
;; - Refactor pub/sub, etc. (note RESP2 vs RESP3 differences)
;; - Refactor helpers API, etc.

;; - Plan for ->v4 upgrade with back compatibility?
;;   - ^{:deprecated <str>}

;; - Final Jedis IO benching (grep for `.read`), and/or remove Jedis code?
;;   - `jedis.RedisInputStream`: readLineBytes, readIntCrLf, readLongCrLf
;;   - `jedis.RedisOutputStream`: writeCrLf, writeIntCrLf

;; - High-level unsimulated (client<->server) tests in dedicated ns

;; - Check all errors: eids, messages, data
;; - Check all dynamic bindings and sys-vals, ensure accessible
;; - v4 wiki with changes, migration, new features, examples, etc.
;; - First alpha release

;; - Could add `to-streaming-freeze` that uses the RESP3 API streaming bulk
;;   type to freeze objects directly to output stream (i.e. without intermediary
;;   ba)? Probably most useful for large objects, but complex, involves tradeoffs,
;;   and how often would this be useful?

;;;; CHANGELOG
;; - [new] Full RESP3 support, incl. streaming, etc.
;; - [new] *auto-serialize?*, *auto-deserialize?*
;; - [new] Greatly improved `skip-replies` performance
;; - [mod] Simplified parsers API
;; - [new] Aggregate  parsers, with xform support

;;;; Config

(def issue-83-workaround?
  "A bug in Carmine v2.6.0 to v2.6.1 (2014-04-01 to 2014-05-01)
  caused Nippy blobs to be marked incorrectly (with `ba-bin` instead
  of `ba-npy`), Ref. https://github.com/ptaoussanis/carmine/issues/83

  This should be kept true (the default) if there's a chance you might
  read any data written by Carmine < v2.6.1.

  Only relevant if `*auto-deserialize?` is true."

  (enc/get-sys-bool true
    "taoensso.carmine.issue-83-workaround"
    "TAOENSSO_CARMINE_ISSUE_83_WORKAROUND"))

(def ^:dynamic *auto-serialize?*
  "Should Carmine automatically serialize arguments sent to Redis
  that are non-native to Redis?

  Affects non-(string, keyword, simple long/double) types.

  If falsey, an exception will be thrown when trying to send
  such arguments.

  Default: true.
  Compile-time default can be overridden with:
    - `taoensso.carmine.auto-serialize` JVM property (read as bool)
    - `TAOENSSO_CARMINE_AUTO_SERIALIZE` env var      (read as bool)

  See also `*auto-deserialize?`*."

  (enc/get-sys-bool true
    "taoensso.carmine.auto-serialize"
    "TAOENSSO_CARMINE_AUTO_SERIALIZE"))

(def ^:dynamic *auto-deserialize?*
  "Should Carmine automatically deserialize Redis replies that
  contain data previously serialized by `*auto-serialize?*`?

  Affects non-(string, keyword, simple long/double) types.

  If false, such replies will by default look like malformed strings.
  TODO: Mention utils, bindings

  Default: true.
  Compile-time default can be overridden with:
    - `taoensso.carmine.auto-deserialize` JVM property (read as bool)
    - `TAOENSSO_CARMINE_AUTO_DESERIALIZE` env var      (read as bool)

  See also `*auto-serialize?`*."

  (enc/get-sys-bool true
    "taoensso.carmine.auto-deserialize"
    "TAOENSSO_CARMINE_AUTO_DESERIALIZE"))

(def ^:dynamic *keywordize-maps?*
  "Keywordize string keys in map-type Redis replies?"
  true)

;;;; Aliases

(do
  (enc/defalias read-com/skip-replies)
  (enc/defalias read-com/normal-replies)
  (enc/defalias read-com/as-bytes)
  (enc/defalias read-com/as-thawed)

  (enc/defalias parsing/unparsed)
  (enc/defalias parsing/parse)
  (enc/defalias parsing/parse-aggregates)
  (enc/defalias parsing/completing-rf)

  (enc/defalias parsing/as-?long)
  (enc/defalias parsing/as-?double)
  (enc/defalias parsing/as-?kw)

  (enc/defalias parsing/as-long)
  (enc/defalias parsing/as-double)
  (enc/defalias parsing/as-kw)

  (enc/defalias        resp/local-echo)
  (enc/defalias return resp/local-echo))

;;;; Scratch

(defn nconn [] (legacy-conns/make-new-connection {:host "127.0.0.1" :port 6379}))
(comment (keys (nconn))) ; (:socket :spec :in :out)

(defn with-carmine
  "Low-level util, prefer `wcar` instead."
  [opts as-vec? body-fn]
  (let [{:keys [conn]} opts
        {:keys [in out]} (or conn (nconn))]
    (resp/with-replies in out as-vec?
      body-fn)))

(defmacro wcar
  "TODO Docstring"
  {:arglists '([opts & body] [opts :as-vec & body])}
  [opts & body]
  (let [[as-vec? body] (resp/parse-body body)]
    `(with-carmine ~opts ~as-vec?
       (fn [] ~@body))))

(comment :see-tests)

(defmacro with-replies
  "TODO Docstring"
  {:arglists '([& body] [:as-vec & body])}
  [& body]
  (let [[as-vec? body] (resp/parse-body body)]
    `(resp/with-replies ~as-vec?
       (fn [] ~@body))))

(comment :see-tests)

(comment
  (wcar {} (resp/redis-request ["SET" "k1" 3]))
  (wcar {} (resp/redis-request ["GET" "k1"]))
  (wcar {}         (resp/ping))
  (wcar {} :as-vec (resp/ping))

  ;; 234.77
  (let [opts {:conn (nconn)}]
    (enc/qb 1e4 (wcar opts (resp/ping)))))

;;;;

(comment
  (legacy-protocol/with-context (nconn)
    (legacy-protocol/with-replies
      (legacy-cmds/enqueue-request 1 ["SET" "KX" "VY"])
      (legacy-cmds/enqueue-request 1 ["GET" "KX"])))

  (let [c (nconn)] (.read (:in c))) ; Inherently blocking
  (let [c (nconn)] (legacy-conns/close-conn c) (.read (:in c))) ; Closed
  )
