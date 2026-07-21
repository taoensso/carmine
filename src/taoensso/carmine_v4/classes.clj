(ns ^:no-doc taoensso.carmine-v4.classes
  "Private classes and interfaces. Kept separate to prevent identity problems
  during REPL work.")

(definterface ReplyError)

(definterface ReusableConnError)

(definterface AttributedReply
  (replyContent [])
  (replyAttributes []))

(definterface RespResourceError)

(definterface RespInput
  (respInputState []))

(definterface RespBuffer
  (^bytes buf      [])
  (^long  pos      [])
  (^long  lim      [])
  (^long  more     [])
  (^long  bufferLine [^long max-line-bytes])
  (^void  consume  [^long n])
  (^long  readByte [])
  (^long  read     [^bytes ba ^long off ^long len])
  (^long  skip     [^long n])
  (^long  available []))

(definterface RawRedisArg
  (^bytes redisBytes []))
