(ns taoensso.carmine.tundra.disk
  "Simple disk-based DataStore implementation for Tundra."
  {:author "Peter Taoussanis"}
  (:require [taoensso.timbre :as timbre]
            [taoensso.carmine.tundra :as tundra])
  (:import  [taoensso.carmine.tundra IDataStore]
            [java.nio.file CopyOption Files LinkOption OpenOption Path Paths
             StandardCopyOption StandardOpenOption NoSuchFileException]))

;;;; Utils

(defn- uuid [] (java.util.UUID/randomUUID))
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
  (fetch-keys [this ks]
    (let [fetch1 (fn [k] (read-ba (format "%s/%s" (path* path)
                                   (tundra/>safe-keyname k))))]
      (mapv #(tundra/catcht (fetch1 %)) ks)))
  (put-key  [this k v]
    (let [result
          (try (let [path-full-temp (format "%s/tmp-%s" (path* path) (uuid))
                     path-full      (format "%s/%s"     (path* path)
                                      (tundra/>safe-keyname k))]
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
  (String. (first (tundra/fetch-keys dstore ["foo:bar:baz"])))

  (time (dotimes [_ 10000]
    (tundra/put-key    dstore "foo:bar:baz" (.getBytes "hello world"))
    (tundra/fetch-keys dstore ["foo:bar:baz"]))))
