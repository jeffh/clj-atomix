(ns net.jeffhui.atomix.log
  (:require [net.jeffhui.atomix.protocols :as protocols]
            [net.jeffhui.atomix.utils :as utils]
            [net.jeffhui.atomix.types :as t])
  (:import io.atomix.core.log.DistributedLogBuilder
           io.atomix.core.log.DistributedLogType
           io.atomix.core.log.DistributedLog
           io.atomix.core.log.AsyncDistributedLog
           io.atomix.core.log.DistributedLogPartition
           io.atomix.core.log.AsyncDistributedLogPartition
           io.atomix.core.Atomix
           io.atomix.core.log.Record
           io.atomix.core.value.AtomicValue
           io.atomix.protocols.log.DistributedLogProtocol))


(defn distributed ^DistributedLog [^Atomix agent {:keys [protocol] :as config}]
  (.build
   ^DistributedLogBuilder
   (t/configure-primitive
    (cond-> (.logBuilder agent)
      protocol (.withProtocol (protocols/->log-protocol protocol)))
    config))
  #_
  (let [^DistributedLogBuilder b (.primitiveBuilder agent (str name) (DistributedLogType/instance))]
    (.build
     ^DistributedLogBuilder
     (t/configure-primitive
      (cond-> b
        protocol (.withProtocol (protocols/->log-protocol protocol)))
      config))))

(defn produce [log-or-partition entry]
  (cond
    (instance? DistributedLog log-or-partition)
    (.produce ^DistributedLog log-or-partition entry)

    (instance? AsyncDistributedLog log-or-partition)
    (.produce ^AsyncDistributedLog log-or-partition entry)

    (instance? DistributedLogPartition log-or-partition)
    (.produce ^DistributedLogPartition log-or-partition entry)

    (instance? AsyncDistributedLogPartition log-or-partition)
    (.produce ^AsyncDistributedLogPartition log-or-partition entry)

    :else (throw (IllegalArgumentException. "Invalid log type"))))

(defn- record->map [partition-id ^Record r]
  {:offset    (.offset r)
   :timestamp (.timestamp r)
   :value     (.value r)
   :partition partition-id})

(defn partitions [log]
  (cond
    (instance? DistributedLog log)
    (into [] (.getPartitions ^DistributedLog log))

    (instance? AsyncDistributedLog log)
    (into [] (.getPartitions ^AsyncDistributedLog log))

    :else (IllegalArgumentException. "Invalid log type")))

(defn partition-id [partition]
  (cond
    (instance? DistributedLogPartition partition)
    (.id ^DistributedLogPartition partition)

    (instance? AsyncDistributedLogPartition partition)
    (.id ^AsyncDistributedLogPartition partition)

    :else (throw (IllegalArgumentException. "Invalid log type"))))

(defn consume! [partition starting-offset f] ;; f := (fn [{:offset int, :timestamp int, value Any}])
  (let [id (partition-id partition)]
    (cond
      (instance? DistributedLogPartition partition)
      (.consume ^DistributedLogPartition partition (long starting-offset) (utils/consumer (comp f (partial record->map id))))

      (instance? AsyncDistributedLogPartition partition)
      (.consume ^AsyncDistributedLogPartition partition (long starting-offset) (utils/consumer (comp f (partial record->map id))))

      :else (throw (IllegalArgumentException. "Invalid log type")))))

(defn atomic-value-committer [^AtomicValue av]
  (fn commit [partition-id offset]
    (let [old (.get av)]
      (when (> offset old)
        (.compareAndSet old offset)))))

(defn atomic-map-committer [^java.util.Map Map am]
  (fn commit [partition-id offset]
    (.compute am partition-id
              (utils/bifunc
               (fn [k v]
                 (cond
                   (nil? v)     offset
                   (< v offset) offset
                   :else        v))))))


(defn stateful-consume! [partition starting-offset commit f]
  (consume! partition starting-offset
            (fn [record]
              (let [res (f record)]
                (commit (:partition record) (:offset record))
                res)))
  (cond
    (instance? DistributedLogPartition partition)
    (.consume ^DistributedLogPartition partition (long starting-offset) (utils/consumer (comp f record->map)))

    (instance? AsyncDistributedLogPartition partition)
    (.consume ^AsyncDistributedLogPartition partition (long starting-offset) (utils/consumer (comp f record->map)))

    :else (throw (IllegalArgumentException. "Invalid log type"))))
