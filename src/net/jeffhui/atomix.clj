(ns net.jeffhui.atomix
  (:require [net.jeffhui.atomix.messaging :as msg]
            [net.jeffhui.atomix.pubsub :as pubsub]
            [net.jeffhui.atomix.utils :as utils]
            [clojure.string :as string]
            [clojure.java.io :as io])
  (:import [io.atomix.core Atomix]
           [io.atomix.cluster Node MemberId Member MemberConfig]
           [io.atomix.core.profile Profile]
           [io.atomix.cluster.discovery BootstrapDiscoveryProvider]
           [io.atomix.protocols.raft.partition RaftPartitionGroup]
           [io.atomix.protocols.backup.partition PrimaryBackupPartitionGroup]
           [io.atomix.storage StorageLevel]
           io.atomix.primitive.partition.MemberGroupStrategy
           io.atomix.cluster.protocol.HeartbeatMembershipProtocol
           io.atomix.cluster.protocol.SwimMembershipProtocol
           io.atomix.utils.net.Address
           io.atomix.protocols.log.partition.LogPartitionGroup
           [java.util.concurrent Executors CompletableFuture]
           [java.util.function Function Consumer]
           [io.atomix.protocols.raft.partition RaftPartitionGroup]
           [io.atomix.primitive.partition ManagedPartitionGroup]))

(set! *warn-on-reflection* true)

(defn ^MemberId ->member-id
  "Generates a atomix MemberId type from a string or node."
  [id]
  (cond
    (instance? MemberId id) id
    (instance? Node id)     (.id ^Node id)
    :else                   (MemberId/from (str id))))

(defn node-id [^Node n] (.id n))
(defn node-address [^Node n]
  (let [^Address addr (.address n)]
    [(.host addr) (.port addr)]))

(defn ->node ^Node [m-or-v]
  (cond
    (instance? Node m-or-v) m-or-v
    (map? m-or-v)
    (let [{:keys [id host port]} m-or-v]
      (cond-> (Node/builder)
        id                    (.withId (str id))
        (and host port)       (.withAddress (str host) (int port))
        (and (not host) port) (.withAddress (int port))
        (and host (not port)) (.withAddress (str host))
        :always               (.build)))
    :else
    (let [[id host port] m-or-v]
      (cond-> (Node/builder)
        id                    (.withId (str id))
        (and host port)       (.withAddress (str host) (int port))
        (and (not host) port) (.withAddress (int port))
        (and host (not port)) (.withAddress (str host))
        :always               (.build)))))

(defn ^BootstrapDiscoveryProvider bootstrap-discover [nodes]
  (let [b (BootstrapDiscoveryProvider/builder)]
    (.build (.withNodes b ^java.util.Collection (mapv ->node nodes)))))

(defn- ->storage-level ^StorageLevel [v]
  (if (instance? StorageLevel v)
    v
    (StorageLevel/valueOf (string/upper-case (name v)))))

(defn ^RaftPartitionGroup raft [name members {:keys [data-dir num-partitions partition-size segment-size flush-on-commit? max-entry-size
                                                     storage-level]}]
  (-> (RaftPartitionGroup/builder (str name))
      (.withMembers ^java.util.Collection (mapv (comp str ->member-id) members))
      (cond->
          data-dir (.withDataDirectory (io/file data-dir))
          num-partitions                (.withNumPartitions (int num-partitions))
          partition-size                (.withPartitionSize (int partition-size))
          segment-size                  (.withSegmentSize (long segment-size))
          max-entry-size                (.withMaxEntrySize (int max-entry-size))
          storage-level                 (.withStorageLevel (->storage-level storage-level))
          (not (nil? flush-on-commit?)) (.withFlushOnCommit flush-on-commit?))
      (.build)))

(defn- ->member-group-strategy
  "
   v := {:host_aware :node_aware :rack_aware :zone_aware}"
  ^MemberGroupStrategy [v]
  (if (instance? MemberGroupStrategy v)
    v
    (MemberGroupStrategy/valueOf (string/upper-case (name v)))))

(defn ^PrimaryBackupPartitionGroup primary-backup [name {:keys [num-partitions member-group-strategy]}]
  (-> (PrimaryBackupPartitionGroup/builder (str name))
      (cond->
       member-group-strategy (.withMemberGroupStrategy (->member-group-strategy member-group-strategy))
       num-partitions (.withNumPartitions (int num-partitions)))
      (.build)))

(defn ^Profile profile-consensus [& member-ids] (Profile/consensus ^java.util.Collection member-ids))
(defn ^Profile profile-grid
  ([] (Profile/dataGrid))
  ([num-partitions] (Profile/dataGrid (int num-partitions))))
(defn profile-client ^Profile [] (Profile/client))

(defn- ->membership-protocol ^io.atomix.cluster.protocol.GroupMembershipProtocol [options]
  (cond
    (instance? io.atomix.cluster.protocol.GroupMembershipProtocol options)
    options

    (:heartbeat options)
    (let [{:keys [failure-threshold failure-timeout heartbeat-interval]} (:heartbeat options)]
      (.build (cond-> (HeartbeatMembershipProtocol/builder)
                failure-threshold  (.withFailureThreshold (int failure-threshold))
                failure-timeout    (.withFailureTimeout (utils/->duration failure-timeout))
                heartbeat-interval (.withHeartbeatInterval (utils/->duration heartbeat-interval)))))

    (:swim options)
    (let [{:keys [broadcast-disputes? broadcast-updates? failure-timeout gossip-fanout gossip-interval notify-suspect? probe-interval suspect-probes]}
          (:swim options)]
      (.build (cond-> (SwimMembershipProtocol/builder)
                (not (nil? broadcast-disputes?)) (.withBroadcastDisputes (boolean broadcast-disputes?))
                (not (nil? broadcast-updates?))  (.withBroadcastUpdates (boolean broadcast-updates?))
                failure-timeout                  (.withFailureTimeout (utils/->duration failure-timeout))
                gossip-fanout                    (.withGossipFanout (int gossip-fanout))
                gossip-interval                  (.withGossipInterval (utils/->duration gossip-interval))
                notify-suspect?                  (.withNotifySuspect (boolean notify-suspect?))
                probe-interval                   (.withProbeInterval (utils/->duration probe-interval))
                suspect-probes                   (.withSuspectProbes (int suspect-probes)))))

    :else (throw (IllegalArgumentException. "Invalid membership protocol"))))

(defn log-group ^LogPartitionGroup [name {:keys [ num-partitions member-group-strategy storage-level data-dir segment-size max-entry-size flush-on-commit? max-size max-age]}]
  (.build
   (cond-> (LogPartitionGroup/builder (str name))
     num-partitions                (.withNumPartitions (int num-partitions))
     member-group-strategy         (.withMemberGroupStrategy (->member-group-strategy member-group-strategy))
     storage-level                 (.withStorageLevel (->storage-level storage-level))
     data-dir                      (.withDataDirectory (io/file data-dir))
     segment-size                  (.withSegmentSize (long segment-size))
     max-entry-size                (.withMaxEntrySize (int max-entry-size))
     (not (nil? flush-on-commit?)) (.withFlushOnCommit (boolean flush-on-commit?))
     max-size                      (.withMaxSize (long max-size))
     max-age                       (.withMaxAge (utils/->duration max-age)))))

(defn- ->group [group]
  (cond
    (instance? ManagedPartitionGroup group) group
    (:raft group)                           (let [opt (:raft group)]
                                              (raft (:name opt) (:members opt) opt))
    (:primary-backup group)                 (let [opt (:primary-backup group)]
                                              (primary-backup (:name opt) opt))
    (:log group)                            (let [opt (:log group)]
                                              (log-group (:name opt) opt))
    :else                                   (throw (IllegalArgumentException. "Invalid group"))))

(defn cluster
  ^Atomix
  [{:keys [cluster-id member-id membership-provider membership-protocol host port
           multicast? management-group partition-groups profiles properties zone rack
           compatible-serialization?]}]
  (.build
   (cond-> (Atomix/builder)
     cluster-id                (.withClusterId (str cluster-id))
     member-id                 (.withMemberId (->member-id member-id))
     membership-protocol       (.withMembershipProtocol (->membership-protocol membership-protocol))
     membership-provider       (.withMembershipProvider ^io.atomix.cluster.discovery.NodeDiscoveryProvider membership-provider)
     management-group          (.withManagementGroup ^ManagedPartitionGroup (->group management-group))
     partition-groups          (.withPartitionGroups ^java.util.Collection (map ->group partition-groups))
     profiles                  (.withProfiles ^java.util.Collection profiles)
     properties                (.withProperties ^java.util.Properties properties)
     compatible-serialization? (.withCompatibleSerialization (boolean compatible-serialization?))
     (and host port)           (.withAddress (str host) (int port))
     (and (not host) port)     (.withAddress (int port))
     (and host (not port))     (.withAddress (str host))
     rack                      (.withRack (str rack))
     zone                      (.withZone (str zone))
     multicast?                (.withMulticastEnabled))))

(defn ^Atomix replica
  ([local-node-id nodes] (replica local-node-id nodes nil))
  ([local-node-id nodes config]
   (let [nodes      (mapv ->node nodes)
         ^Node self (first (filter (comp #{local-node-id} #(str (.id ^Node %))) nodes))]
     (cluster (merge {:membership-provider (bootstrap-discover nodes)
                      :member-id           local-node-id
                      :host                (.host (.address self))
                      :port                (.port (.address self))
                      :membership-protocol {:heartbeat {:heartbeat-interval [10 :seconds]}}
                      :management-group    {:raft {:name           "system"
                                                   :num-partitions 1
                                                   :members        nodes
                                                   :storage-level  :disk
                                                   :data-dir       (str "data/" local-node-id "/system")}}
                      :partition-groups    [{:raft {:name           "raft"
                                                    :num-partitions (* 2 (count nodes))
                                                    :members        nodes
                                                    :storage-level  :disk
                                                    :data-dir       (str "data/" local-node-id "/raft")}}
                                            {:log {:name           "log"
                                                   :num-partitions (* 2 (count nodes))
                                                   :members        nodes
                                                   :storage-level  :disk
                                                   :data-dir       (str "data/" local-node-id "/log")}}]}
                    config)))))

(defn start
  (^CompletableFuture [^Atomix agent] (.start agent))
  ([^Atomix agent {:keys [join?]}]
   (cond-> (.start agent)
     join? (.join))))

(defn stop
  (^CompletableFuture [^Atomix agent] (.stop agent))
  ([^Atomix agent {:keys [join?]}]
   (cond-> (.stop agent)
     join? (.join))))

(defn member->map [^Member m]
  {:id         (str (.id m))
   :properties (into {} (.properties m))
   :active?    (.isActive m)
   :rack       (.rack m)
   :version    (str (.version m))
   :zone       (.zone m)
   :config     (let [^MemberConfig cfg (.config m)]
                 {:address    (str (.getAddress cfg))
                  :host       (.getHost cfg)
                  :id         (str (.getId cfg))
                  :properties (into {} (.getProperties cfg))})})
(defn local-member [^Atomix agent] (member->map (.getLocalMember (.getMembershipService agent))))
(defn members [^Atomix agent] (set (map member->map (.getMembers (.getMembershipService agent)))))
(defn reachable-members [^Atomix agent] (set (map member->map (.getReachableMembers (.getMembershipService agent)))))

(comment
  (do
    (def nodes [["node1" nil 5677]
                ["node2" nil 5678]
                ["node3" nil 5679]])
    (def r1 (replica (first (nodes 0)) nodes))
    (def r2 (replica (first (nodes 1)) nodes))
    (def r3 (replica (first (nodes 2)) nodes)))

  (def cfs (doall (map #(.start %) [r1 r2 r3])))

  (doseq [r [r1 r2 r3]]
    (.stop r))

  (members r2)
  (local-member r2)

  (msg/subscribe-handler! r1 "test" (fn [x] (println "RECV" x) x))
  (msg/unsubscribe! r1 "test")
  (def res (msg/send! r2 "test" "hello world" "node1"))
  (.getNow res :missing)
  (msg/send! r1 "node1" "hello world" "node2")


  (pubsub/subscribe-handler! r1 "test" (fn [x] (println "RECV" x) (str x "world")))
  (pubsub/unsubscribe! r1 "test")
  (pubsub/subscribe-handler! r2 "test" (fn [x] (println "RECV2" x) x))
  (pubsub/unsubscribe! r2 "test")
  (def res (pubsub/send! r2 "test" "hello world"))
  (.getNow res :missing)
  (pubsub/broadcast! r2 "test" "hello world")
  )
