(ns net.jeffhui.atomix.protocols
  (:require [net.jeffhui.atomix.utils :as utils]
            [clojure.string :as string])
  (:import [io.atomix.protocols.raft MultiRaftProtocol ReadConsistency]
           [io.atomix.protocols.raft.session CommunicationStrategy]
           [io.atomix.protocols.backup MultiPrimaryProtocol]
           [io.atomix.protocols.gossip AntiEntropyProtocol PeerSelector PeerSelectors TimestampProvider TimestampProviders CrdtProtocol]
           [io.atomix.primitive.partition Partitioner PartitionId]
           [io.atomix.primitive Recovery Consistency Replication]
           io.atomix.primitive.protocol.ProxyProtocol
           io.atomix.primitive.protocol.set.SetProtocol
           io.atomix.protocols.raft.partition.RaftPartitionGroup
           io.atomix.primitive.protocol.LogProtocol
           io.atomix.protocols.log.DistributedLogProtocol))

(defn ->recovery
  "Converts to a recovery instance.
   v := #{:close :recover}"
  ^Recovery [v]
  (if (instance? Recovery v)
    v
    (Recovery/valueOf (string/upper-case (name v)))))

(defn ->read-consistency
  "Converts to a read-consistency instance.

   v := #{:linearizable :linearizeable_lease :sequential}"
  ^ReadConsistency [v]
  (if (instance? ReadConsistency v)
    v
    (ReadConsistency/valueOf (string/upper-case (name v)))))

(defn ->communication-strategy
  "Converts to communication strategy instance.

   values := #{:any :followers :leader}"
  ^CommunicationStrategy [value]
  (if (instance? CommunicationStrategy value)
    value
    (CommunicationStrategy/valueOf (string/upper-case (name value)))))

(defn ->partition-id ^PartitionId
  ([v]
   (if (instance? PartitionId v)
     v
     (if (or (vector? v) (list? v))
       (apply ->partition-id v)
       (throw (IllegalArgumentException. (format "%s cannot be converted to partition id" (pr-str v)))))))
  ([group id] (PartitionId. (str group) (int id))))

(defn ->partitioner ^Partitioner
  ([] Partitioner/MURMUR3)
  ([f] (reify Partitioner
         (partition [_ key partitions] (->partition-id (f key partitions))))))

(defn multiraft
  "Returns configuration for the raft protocol"
  ^MultiRaftProtocol
  [{:keys [group max-retries max-timeout min-timeout
           communication-strategy partitioner read-consistency
           recovery-strategy retry-delay]}]
  (.build
   (cond-> (if group
             (MultiRaftProtocol/builder (str group))
             (MultiRaftProtocol/builder))
     max-retries (.withMaxRetries (int max-retries))
     max-timeout (.withMaxTimeout (utils/->duration max-timeout))
     min-timeout (.withMaxTimeout (utils/->duration min-timeout))
     retry-delay (.withRetryDelay (utils/->duration retry-delay))
     communication-strategy (.withCommunicationStrategy (->communication-strategy communication-strategy))
     partitioner (.withPartitioner (->partitioner partitioner))
     read-consistency (.withReadConsistency (->read-consistency read-consistency))
     recovery-strategy (.withRecoveryStrategy (->recovery recovery-strategy)))))

(defn ->consistency
  "Converts to a consistency instance.
   v := #{:eventual :linearizable :sequential}"
  ^Consistency [v]
  (if (instance? Consistency v)
    v
    (Consistency/valueOf (string/upper-case (name v)))))

(defn ->replication
  "Converts to a replication instance.
   v := #{:asynchronous :synchronous}"
  ^Replication [v]
  (if (instance? Replication v)
    v
    (Replication/valueOf (string/upper-case (name v)))))

(defn multiprimary
  ^MultiPrimaryProtocol
  [{:keys [group backups max-retries consistency partitioner
           recovery replication retry-delay]}]
  (.build
   (cond-> (if group
             (MultiPrimaryProtocol/builder (str group))
             (MultiPrimaryProtocol/builder))
     backups (.withBackups (int backups))
     max-retries (.withMaxRetries (int max-retries))
     consistency (.withConsistency (->consistency consistency))
     partitioner (.withPartitioner (->partitioner partitioner))
     recovery (.withRecovery (->recovery recovery))
     replication (.withReplication (->replication replication))
     retry-delay (.withRetryDelay (utils/->duration retry-delay)))))

(defn ->multi ^ProxyProtocol [{:keys [primary raft]}]
  (if raft
    (multiraft raft)
    (multiprimary primary)))

(defn ->peer-selector ^PeerSelector [f]
  (cond
    (instance? PeerSelector f) f
    (keyword? f) (PeerSelectors/valueOf (string/upper-case (name f)))
    :else (reify PeerSelector
            (select [this entry membership] (f entry membership)))))

(defn ->timestamp-provider
  "v := #{TimestampProvider :wall_clock (fn [entry] ..<io.atomix.utils.time.Timestamp>..)}"
  ^TimestampProvider [v]
  (cond
    (instance? TimestampProvider v) v
    (keyword? v) (TimestampProviders/valueOf (string/upper-case (name v)))
    :else (reify TimestampProvider
            (get [this entry] (v entry)))))

(defn anti-entropy [{:keys [gossip-interval anti-entropy-interval peers
                            peer-selector timestamp-provider
                            tombstones-disabled?]}]
  (cond-> (AntiEntropyProtocol/builder)
    anti-entropy-interval (.withAntiEntropyInterval (utils/->duration anti-entropy-interval))
    gossip-interval (.withGossipInterval (utils/->duration gossip-interval))
    peers (.withPeers (set peers))
    peer-selector (.withPeerSelector (->peer-selector peer-selector))
    timestamp-provider (.withTimestampProvider timestamp-provider)
    (not (nil? tombstones-disabled?)) (.withTombstonesDisabled (boolean tombstones-disabled?))))

(defn crdt [{:keys [gossip-interval timestamp-provider]}]
  (.build
   (cond-> (CrdtProtocol/builder)
     gossip-interval (.withGossipInterval (utils/->duration gossip-interval))
     timestamp-provider (.withTimestampProvider (->timestamp-provider timestamp-provider)))))

(defn ->set ^SetProtocol [options]
  (cond
    (:anti-entropy options) (anti-entropy (:anti-entropy options))
    (:crdt options) (crdt (:crdt options))
    :else nil))

(defn ->log-protocol ^LogProtocol [{:keys [name partitioner consistency replication recovery max-retries retry-delay] :as options}]
  (.build
   (cond-> (if name
             (DistributedLogProtocol/builder (str name))
             (DistributedLogProtocol/builder))
     partitioner (.withPartitioner (->partitioner partitioner))
     consistency (.withConsistency (->consistency consistency))
     replication (.withReplication (->replication replication))
     recovery (.withRecovery (->recovery recovery))
     max-retries (.withMaxRetries (int max-retries))
     retry-delay (.withRetryDelay (utils/->duration retry-delay)))))
