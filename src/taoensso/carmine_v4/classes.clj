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
  (respLimitState []))

(definterface RawRedisArg
  (^bytes redisBytes []))
