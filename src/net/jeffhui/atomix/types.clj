(ns net.jeffhui.atomix.types
  (:require [net.jeffhui.atomix.utils :as utils :refer [*default-serializer*]]
            [net.jeffhui.atomix.protocols :as protocols]
            [clojure.string :as string])
  (:import [io.atomix.core Atomix]
           [io.atomix.core.value AtomicValue DistributedValue]
           [io.atomix.core.set DistributedSet]
           [io.atomix.core.workqueue WorkQueue]
           [io.atomix.primitive PrimitiveBuilder SyncPrimitive AsyncPrimitive]
           [io.atomix.utils.event EventListener]
           [io.atomix.core.map MapEventListener MapEvent]
           [io.atomix.core.queue DistributedQueue]
           [io.atomix.core.map DistributedMap AtomicMap]
           [io.atomix.core.idgenerator AtomicIdGenerator]
           [io.atomix.core.tree AtomicDocumentTree]
           io.atomix.core.collection.DistributedCollectionBuilder
           io.atomix.core.cache.CachedPrimitiveBuilder
           io.atomix.primitive.protocol.set.SetProtocol
           io.atomix.core.election.LeaderElection
           io.atomix.core.election.Leader
           io.atomix.core.election.Leadership
           io.atomix.core.election.LeadershipEventListener
           io.atomix.core.election.LeadershipEvent
           io.atomix.core.election.LeadershipEvent$Type
           io.atomix.utils.time.Versioned
           java.util.concurrent.Executors
           java.util.concurrent.CompletableFuture))

(defn ->event-listener ^EventListener [f]
  (reify EventListener
    (event [self e] (f self e))))

(defn ->map-event-listener ^MapEventListener [apply-f]
  (reify MapEventListener
    (event [self e] (apply-f self e))
    #_(isRelevant [self e] (filter-f self e))))

(defn leader->map [^Leader l]
  {:id        (.id l)
   :term      (.term l)
   :timestamp (.timestamp l)})

(defn leadership->map [^Leadership ls]
  {:candidates (seq (.candidates ls))
   :leader     (leadership->map (.leader ls))})

(defn leadership-event->map [^LeadershipEvent e]
  {:event/type     (keyword (str (.type e)))
   :event/time     (.time e)
   :new-leadership (leadership-event->map (.newLeadership e))
   :old-leadership (leadership-event->map (.oldLeadership e))
   :topic          (.topic e)})

(defn ->leadership-event-listener ^LeadershipEventListener [apply-f]
  (reify LeadershipEventListener
    (event [self e] (apply-f self (leadership-event->map e)))))

(defn ->state-change-listener
  "f := (fn [state := #{:closed :connected :expired :suspended}] ...)"
  ^java.util.function.Consumer [f]
  (utils/consumer (comp f keyword string/lower-case str)))

(defn- maybe-configure-cached-primitive [b {:keys [cache-enabled? cache-size]}]
  (if (instance? CachedPrimitiveBuilder b)
    (cond-> b
      cache-enabled? (.withCacheEnabled (boolean cache-enabled?))
      cache-size     (.withCacheSize (int cache-size)))
    b))

(defn- maybe-configure-collection [b {:keys [compatible-serialization? registration-required?]}]
  (if (instance? DistributedCollectionBuilder b)
    (cond-> b
      compatible-serialization? (.withCompatibleSerialization b (boolean compatible-serialization?))
      registration-required?    (.withRegistrationRequired b (boolean registration-required?)))
    b))

(defn configure-primitive
  [^PrimitiveBuilder b {:keys [read-only? serializer]
                        :or   {serializer *default-serializer*}
                        :as   options}]
  (cond-> (-> b
              (maybe-configure-collection options)
              (maybe-configure-cached-primitive options))
    read-only? (.withReadOnly (boolean read-only?))
    serializer (.withSerializer serializer)))

(defn atomic-value ^AtomicValue [^Atomix agent name {:keys [protocol] :as options}]
  (.build
   (configure-primitive
    (cond-> (.atomicValueBuilder agent (str name))
      protocol (.withProtocol (protocols/->multi protocol)))
    options)))

(defn atomic-map ^AtomicMap [^Atomix agent name {:keys [protocol] :as options}]
  (.build
   (configure-primitive
    (cond-> (.atomicMapBuilder agent (str name))
      protocol (.withProtocol (protocols/->multi protocol)))
    options)))

(defn atomic-doc-tree ^AtomicDocumentTree [^Atomix agent name {:keys [protocol] :as options}]
  (.build
   (configure-primitive
    (cond-> (.atomicDocumentTreeBuilder agent (str name))
      protocol (.withProtocol (protocols/->multi protocol)))
    options)))

(defn distributed-value ^DistributedValue [^Atomix agent name {:keys [protocol] :as options}]
  (.build
   (configure-primitive
    (cond-> (.distributedValueBuilder agent (str name))
      protocol (.withProtocol (protocols/->multi protocol)))
    options)))

(defn distributed-queue ^DistributedQueue [^Atomix agent name {:keys [protocol] :as options}]
  (.build
   (configure-primitive
    (cond-> (.queueBuilder agent (str name))
      protocol (.withProtocol (protocols/->multi protocol)))
    options)))

(defn atomic-id-generator ^AtomicIdGenerator [^Atomix agent name {:keys [protocol] :as options}]
  (.build
   (configure-primitive
    (cond-> (.atomicIdGeneratorBuilder agent (str name))
      protocol (.withProtocol (protocols/->multi protocol)))
    options)))

(defn distributed-map ^DistributedMap [^Atomix agent name {:keys [protocol allow-nil?] :as options}]
  (.build
   (configure-primitive
    (cond-> (.mapBuilder agent (str name))
      (:anti-entropy protocol)                       (.withProtocol (protocols/anti-entropy (:anti-entropy protocol)))
      (and protocol (nil? (:anti-entropy protocol))) (.withProtocol (protocols/->multi protocol))
      (not (nil? allow-nil?))                        (.withNullValues (boolean allow-nil?)))
    options)))

(defn distributed-set ^DistributedSet [^Atomix agent name {:keys [protocol] :as options}]
  (.build
   (configure-primitive
    (let [^SetProtocol set-protocol (protocols/->set protocol)]
      (cond-> (.buildSet agent (str name))
        set-protocol                      (.withProtocol set-protocol)
        (and (not set-protocol) protocol) (.withProtocol (protocols/->multi protocol))))
    options)))

(defn work-queue
  ^WorkQueue [^Atomix agent name {:keys [processor parallism executor protocol]
                                  :as   options}]
  (let [q (.build
           (configure-primitive
            (cond-> (.workQueueBuilder agent (str name))
              protocol (.withProtocol (protocols/->multi protocol)))
            options))]
    (when processor
      (.registerTaskProcessor q
                              (utils/consumer processor)
                              (or parallism 1)
                              (or executor (Executors/newSingleThreadExecutor))))
    q))

(defn stop-work-queue [^WorkQueue q]
  (.stopProcessing q))

(defn ->async ^AsyncPrimitive [^SyncPrimitive p] (.async p))
(defn ->sync ^SyncPrimitive [^AsyncPrimitive a] (.sync a))
(defn close [value]
  (if (instance? AsyncPrimitive value)
    (.close ^AsyncPrimitive value)
    (do (.close ^SyncPrimitive value)
        (CompletableFuture/completedFuture nil))))

(defn versioned->map [^Versioned v]
  {:value         (.value v)
   :version       (.version v)
   :creation-time (.creationTime v)})

(defn leader-election ^LeaderElection [^Atomix agent name {:keys [protocol] :as options}]
  (.build
   (configure-primitive
    (cond-> (.leaderElectionBuilder agent (str name))
      protocol (.withProtocol (protocols/->multi protocol)))
    options)))
