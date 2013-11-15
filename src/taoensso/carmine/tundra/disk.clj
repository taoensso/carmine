(ns taoensso.carmine.tundra.disk
  "Simple disk-based DataStore implementation for Tundra."
  {:author "Peter Taoussanis"}
  (:require [taoensso.timbre :as timbre])
  (:import  [taoensso.carmine.tundra IDataStore]
            [java.net URLDecoder URLEncoder]
            [java.nio.file CopyOption Files LinkOption OpenOption Path Paths
             StandardCopyOption StandardOpenOption NoSuchFileException]))

;;;; Utils

(defn- uuid [] (java.util.UUID/randomUUID))
(defn- >fname-safe [s] (URLEncoder/encode (str s) "ISO-8859-1"))
(defn- <fname-safe [s] (URLDecoder/decode (str s) "ISO-8859-1"))
(comment (<fname-safe (>fname-safe "hello f8 8 93#**#\\// !!$")))

(defn- path*  [path] (Paths/get "" (into-array String [path])))
(defn- mkdirs [path] (.mkdirs ^java.io.File (.toFile ^Path (path* path))))
(defn- mv [path-source path-dest]
  (Files/move (path* path-source) (path* path-dest)
    (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE
                            StandardCopyOption/REPLACE_EXISTING])))

(defn- read-ba  [path] (Files/readAllBytes (path* path)))
(defn- write-ba [path ba]
  (Files/write (path* path) ba
    (into-array OpenOption [StandardOpenOption/CREATE
                            StandardOpenOption/TRUNCATE_EXISTING
                            StandardOpenOption/WRITE
                            StandardOpenOption/SYNC])))

;;;;

(defrecord DiskDataStore [path]
  IDataStore
  (fetch-key [this k] (read-ba (format "%s/%s" (path* path) (>fname-safe k))))
  (put-key   [this k v]
    (let [result
          (try (let [path-full-temp (format "%s/tmp-%s" (path* path) (uuid))
                     path-full      (format "%s/%s"     (path* path) (>fname-safe k))]
                 (write-ba path-full-temp v)
                 (mv       path-full-temp path-full))
               (catch Exception e e))]

      (cond
       (instance? Path result) true
       (instance? NoSuchFileException result)
       (do (mkdirs path) (recur k v))

       (instance? Exception result) result
       :else (Exception. (format "Unexpected result: %s" result))))))

(defn disk-datastore
  "Alpha - subject to change.
  Requires JVM 1.7+.
  Supported Freezer io types: byte[s]."
  [path] {:pre [(string? path)]} (->DiskDataStore path))

(comment
  (require '[taoensso.carmine.tundra :as tundra])
  (def dstore (disk-datastore "./tundra"))
  (tundra/put-key dstore "foo:bar:baz" (.getBytes "hello world"))
  (String. (tundra/fetch-key dstore "foo:bar:baz"))

  (time (dotimes [_ 10000]
    (tundra/put-key   dstore "foo:bar:baz" (.getBytes "hello world"))
    (tundra/fetch-key dstore "foo:bar:baz"))))
