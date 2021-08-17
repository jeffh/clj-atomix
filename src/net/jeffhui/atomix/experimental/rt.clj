(ns net.jeffhui.atomix.experimental.rt
  (:require [net.jeffhui.atomix.types :as t]
            [net.jeffhui.atomix.utils :as utils])
  (:import [io.atomix.core Atomix]
           [io.atomix.core.workqueue WorkQueue AsyncWorkQueue]
           [io.atomix.core.idgenerator AtomicIdGenerator]
           [io.atomix.core.map DistributedMap MapEvent]
           io.atomix.core.set.DistributedSet
           io.atomix.core.workqueue.WorkQueue
           java.util.concurrent.CompletableFuture))

(defrecord Service [^String name
                    ^DistributedSet responses
                    ^AtomicIdGenerator id-gen
                    ^WorkQueue queue
                    ^DistributedSet registry]
  java.io.Closeable
  (close [_]
    (.close queue)
    (.close responses)
    (.close id-gen)))

(defn ^DistributedSet service-registry
  ([^Atomix agent] (service-registry "net.jeffhui.atomix.default-registry"))
  ([^Atomix agent name]
   (t/distributed-set agent (str name) {:protocols      {:primary {:backups     2
                                                                   :consistency :eventual
                                                                   :recovery    :recover}}
                                        :cache-enabled? true
                                        :cache-size     64})))

(def ^:private responses-options
  {:protocols  {:primary {:backups     1
                          :max-retries 3
                          :retry-delay [2 :seconds]
                          :consistency :eventual
                          :recovery    :recover}}
   :allow-nil? true})

(defn run-service [^Atomix agent name {:keys [registry]
                                       :as   options}
                   processor]
  (let [idgen     (t/atomic-id-generator agent (str name ":id_gen") nil)
        responses (t/distributed-map agent (str name ":responses") responses-options)]
    (.add (or registry (service-registry agent)) (str name))
    (->Service (str name)
               responses
               idgen
               (t/work-queue agent (str name ":queue")
                             (assoc options :processor (fn [{:keys [response-id payload]}]
                                                         (let [response (processor payload)]
                                                           (when response-id
                                                             (.put responses response-id response
                                                                   (utils/->duration [1 :hours])))))))
               registry)))

(defn delete-service
  ([^DistributedSet registry service]
   (.remove registry (str (:name service)))
   (.delete (:queue service))
   (.delete (:responses service))
   (.delete (:id-gen service))))

(defn find-service
  ([^Atomix agent name] (find-service agent (service-registry agent) name))
  ([^Atomix agent ^DistributedSet registry name]
   (when (.contains registry (str name))
     (let [idgen     (t/atomic-id-generator agent (str name ":id_gen") nil)
           responses (t/distributed-map agent (str name ":responses") responses-options)]
       (->Service (str name)
                  responses
                  idgen
                  (t/work-queue agent (str name ":queue") nil)
                  registry)))))

(defn service-request ^CompletableFuture [service request]
  (let [response-id (.nextId ^AtomicIdGenerator (:id-gen service))
        result      (promise)
        ^DistributedMap responses   (:responses service)]
    (.addListener
     responses
     (t/->map-event-listener (fn [listener ^MapEvent event]
                               (when (= response-id (.key event))
                                 (deliver result (.newValue event))
                                 (.removeListener responses listener)))))
    (.addOne ^WorkQueue (:queue service) {:response-id response-id
                                          :payload     request})
    result))

(comment
  (do
    (do
      (require '[net.jeffhui.atomix :as atomix])
      (def nodes [["node1" "localhost" 5677]
                  ["node2" "localhost" 5678]
                  ["node3" "localhost" 5679]])
      (def r1 (atomix/replica (first (nodes 0)) nodes))
      (def r2 (atomix/replica (first (nodes 1)) nodes))
      (def r3 (atomix/replica (first (nodes 2)) nodes)))

    (def cfs (doall (map #(.start %) [r1 r2 r3]))))
  cfs

  (doseq [r [r1 r2 r3]] (.stop r))

  (atomix/members r1)
  (.isRunning r1)
  (.start r1)
  (.stop r1)

  (atomix/members r1)

  (def c (atomix/client "c1"
                        [["mgmt1" "mgmt1" 6600]
                         ["mgmt2" "mgmt2" 6601]
                         ["mgmt3" "mgmt3" 6602]
                         ["c1" "localhost" 5679]]
                        {:cluster-id       "cluster"
                         :management-group {:raft {:name           "system"
                                                   :num-partitions 1
                                                   :members        ["mgmt1" "mgmt2" "mgmt3"]}}}))
  (def cf (atomix/start c))
  cf
  (.get cf)
  (.isRunning c)
  (atomix/stop c)
  (atomix/members c)

  (def r1-echo-service (run-service r1 "echo" {} (fn [msg] msg)))
  (.close r1-echo-service)

  (.getConfigService c)
  (into [] (.getPartitionGroups (.getPartitionService c)))

  (def test-map (t/atomic-map c "test-map" {:protocol {:raft {:name             "raft"
                                                              :read-consistency :linearizable}}}))
  (.delete test-map)
  (.close test-map)
  (def test-map (t/distributed-map c "test2-dmap" {:protocol   {:primary {:backups 2}}
                                                   :allow-nil? true}))
  (.delete test-map)
  (.close test-map)

  (def atest-map (t/->async test-map))

  (def res (.put atest-map "test" "yes"))
  res
  (.put test-map "test" "yes")

  (.size test-map)

  (time
   (dotimes [i 100000]
     (.put test-map (str "key-" i) {:index i
                                    :value (str "key-" i)})))

  (time (t/versioned->map (.get test-map (str "key-" 0))))

  (require '[net.jeffhui.atomix.log :as log] :reload)
  (def lg (log/distributed r1 {:protocol {:name        "log"
                                          :consistency :linearizable
                                          :replication :synchronous}}))

  (def partition-state (t/atomic-map r1 "partitions" {:protocol {:raft {:read-consistency :linearizable}}}))

  (def queue (t/work-queue c "test-work-queue" {:protocol {:primary {:backups     1
                                                                :recovery    :recover
                                                                :replication :synchronous}}}))
  (.close queue)
  (time
   (dotimes [i 10000]
     (.poll queue)))

  (time
   (dotimes [i 10000]
     (.add queue {:index i :value (str "key-" i)})))

  (time
   (.addAll queue (mapv (fn [i] {:index i :value (str "key-" i)}) (range 10000))))

  

  (into
   {}
   (map (fn [[k v]] [k (t/versioned->map v)]))
   (.getAllPresent partition-state (.keySet partition-state)))

  (t/close lg)

  (dotimes [i 1000] (log/produce lg (str i)))
  (def all-parts (log/partitions lg))
  (def outs (doall (map #(log/stateful-consume! % 0 (log/atomic-map-committer partition-state) (fn [m] (prn "RECV" m))) all-parts)))
  (def outs (doall (map #(log/consume! % 0 (fn [m] (prn "RECV" m))) all-parts)))

  (def r-echo-service (run-service r1 "echo" {} (fn [msg] msg)))
  (def res (service-request r-echo-service {:hello "world"}))
  (def s1 (find-service r2 "echo"))
  (def res (service-request s1 {:hello "world2"}))
  (deref res 1000 :timeout)
  (.close r-echo-service)
  )
