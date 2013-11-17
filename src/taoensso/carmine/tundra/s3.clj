(ns taoensso.carmine.tundra.s3
  "AWS S3 (clj-aws-s3) DataStore implementation for Tundra."
  {:author "Peter Taoussanis"}
  (:require [clojure.string  :as str]
            [aws.sdk.s3      :as s3]
            [taoensso.timbre :as timbre]
            [taoensso.carmine.tundra :as tundra])
  (:import  [java.io ByteArrayInputStream DataInputStream]
            [taoensso.carmine.tundra IDataStore]
            [com.amazonaws.services.s3.model AmazonS3Exception PutObjectResult]))

(defn- base64-md5 [^bytes x]
  (-> x
      (org.apache.commons.codec.digest.DigestUtils/md5)
      (org.apache.commons.codec.binary.Base64/encodeBase64String)))

(defrecord S3DataStore [creds bucket]
  IDataStore
  (put-key [this k v]
    (let [reply (try (s3/put-object creds bucket k (ByteArrayInputStream. v)
                       {:content-length (count v)
                        ;; Nb! Prevents overwriting with corrupted data:
                        :content-md5    (base64-md5 v)})
                     (catch Exception e e))]
      (cond
       (instance? PutObjectResult reply) true
       (and (instance? AmazonS3Exception reply)
            (= (.getMessage ^AmazonS3Exception reply)
               "The specified bucket does not exist"))
       (let [bucket (first (str/split bucket #"/"))]
         (s3/create-bucket creds bucket)
         (recur k v))
       (instance? Exception reply) reply
       :else (Exception. (format "Unexpected reply: %s" reply)))))

  (fetch-keys [this ks]
    (let [fetch1
          (fn [k]
            (let [obj (s3/get-object creds bucket k)
                  ba  (byte-array (-> obj :metadata :content-length))]
              (.readFully (DataInputStream. (:content obj)) ba)
              ba))]
      (mapv #(tundra/catcht (fetch1 %)) ks))))

(defn s3-datastore
  "Alpha - subject to change.
  Returns a Faraday DataStore using given AWS S3 credentials and bucket.

  (s3-datastore {:access-key \"<key>\" :secret-key \"<key\"}
                \"my-bucket/my-folder\")

  Supported Freezer io types: byte[]s."
  [creds bucket]
  (->S3DataStore creds (name bucket)))

(comment
  (require '[taoensso.carmine.tundra :as tundra])
  (def dstore (s3-datastore creds "ensso-store/folder"))
  (s3/put-object creds "ensso-store/folder" "foo:bar:baz" "hello world")
  (tundra/put-key dstore "foo:bar:baz" (.getBytes "hello world"))
  (String. (first (tundra/fetch-keys dstore ["foo:bar:baz"]))))
