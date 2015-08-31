(ns taoensso.carmine.tundra.s3
  "AWS S3 (clj-aws-s3) DataStore implementation for Tundra."
  {:author "Peter Taoussanis"}
  (:require [clojure.string          :as str]
            [aws.sdk.s3              :as s3]
            [taoensso.encore         :as enc :refer (have? have)]
            [taoensso.timbre         :as timbre]
            [taoensso.carmine.tundra :as tundra])
  (:import  [java.io ByteArrayInputStream DataInputStream]
            [taoensso.carmine.tundra IDataStore]
            [com.amazonaws.services.s3.model AmazonS3Exception PutObjectResult]))

(defn- base64-md5 [^bytes x]
  (-> x (org.apache.commons.codec.digest.DigestUtils/md5)
        (org.apache.commons.codec.binary.Base64/encodeBase64String)))

(defrecord S3DataStore [creds bucket]
  IDataStore
  (put-key [this k v]
    (have? enc/bytes? v)
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
       :else (ex-info (format "Unexpected reply: %s" reply) {:reply reply}))))

  (fetch-keys [this ks]
    (let [fetch1
          (fn [k]
            (tundra/catcht
              (let [obj (s3/get-object creds bucket k)]
                (with-open
                    [^com.amazonaws.services.s3.model.S3ObjectInputStream cnt
                     (:content obj)]
                  (let [ba (byte-array (enc/have enc/pos-int?
                                         (-> obj :metadata :content-length)))]
                    (.readFully (DataInputStream. cnt) ba)
                    ba)))))]
      (->> (mapv #(future (fetch1 %)) ks)
           (mapv deref)))))

(defn s3-datastore
  "Alpha - subject to change.
  Returns a Faraday DataStore using given AWS S3 credentials and bucket.

  (s3-datastore {:access-key \"<key>\" :secret-key \"<key\"}
                \"my-bucket/my-folder\")

  Supported Freezer io types: byte[]s."
  [creds bucket] {:pre [(string? bucket)]}
  (S3DataStore. creds bucket))

(comment
  (def dstore  (s3-datastore creds "ensso-store/folder"))
  (def hardkey (tundra/>urlsafe-str "00temp-test-foo:bar /♡\\:baz "))
  (tundra/put-key dstore hardkey (.getBytes "hello world"))
  (String. (first (tundra/fetch-keys dstore [hardkey]))))
