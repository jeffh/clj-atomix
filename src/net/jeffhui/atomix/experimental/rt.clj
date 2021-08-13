(ns net.jeffhui.atomix.experimental.rt
  (:require [net.jeffhui.atomix.types :as t]
            [net.jeffhui.atomix.utils :as utils])
  (:import [io.atomix.core Atomix]
           [io.atomix.core.workqueue WorkQueue AsyncWorkQueue]
           [io.atomix.core.idgenerator AtomicIdGenerator]
           [io.atomix.core.map DistributedMap MapEvent]
           java.util.concurrent.CompletableFuture))

(defrecord Service [name responses id-gen queue]
  java.io.Closeable
  (close [_]
    (.close queue)
    (.close responses)
    (.close id-gen)))

(defn service-registry
  ([^Atomix agent] (service-registry "net.jeffhui.atomix.default-registry"))
  ([^Atomix agent name]
   (t/distributed-map agent (str name) nil)))

(defn run-service [^Atomix agent name options processor]
  (let [idgen     (t/atomic-id-generator agent (str name ":id_gen") nil)
        responses (t/distributed-map agent (str name ":responses") nil)]
    (->Service (str name)
               responses
               idgen
               (t/work-queue agent (str name ":queue")
                             (assoc options :processor (fn [{:keys [response-id payload] :as msg}]
                                                         (when response-id
                                                           (.put responses response-id (processor payload)))))))))

(defn find-service [^Atomix agent name]
  (let [idgen     (t/atomic-id-generator agent (str name ":id_gen") nil)
        responses (t/distributed-map agent (str name ":responses") nil)]
    (->Service (str name)
               responses
               idgen
               (t/work-queue agent (str name ":queue") nil))))

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
    (require '[net.jeffhui.atomix :as atomix])
    (def nodes [["node1" "localhost" 5677]
                ["node2" "localhost" 5678]
                ["node3" "localhost" 5679]])
    (def r1 (atomix/replica (first (nodes 0)) nodes))
    (def r2 (atomix/replica (first (nodes 1)) nodes))
    (def r3 (atomix/replica (first (nodes 2)) nodes)))

  (def cfs (doall (map #(.start %) [r1 r2 r3])))
  cfs

  (doseq [r [r1 r2 r3]] (.stop r))

  (atomix/members r1)
  (.isRunning r1)
  (.start r1)
  (.stop r1)

  (atomix/members r1)

  (def c (atomix/replica "c1"
                         [["mgmt1" "localhost" 6600]
                          ["mgmt2" "localhost" 6601]
                          ["mgmt3" "localhost" 6602]
                          ["c1" "localhost" 5679]]
                         {:cluster-id          "cluster"
                          :management-group    {:raft {:name           "system"
                                                       :num-partitions 1
                                                       :members        ["mgmt1" "mgmt2" "mgmt3"]}}
                          :membership-protocol {:heartbeat {:heartbeat-interval [5 :seconds]}}
                          :partition-groups    [{:raft {:name           "main"
                                                        :num-partitions 6
                                                        :members        ["mgmt1" "mgmt2" "mgmt3"]}}]}))
  (def cf (atomix/start c))
  cf
  (.get cf)
  (atomix/stop c)

  (def r1-echo-service (run-service r1 "echo" {} (fn [msg] msg)))
  (.close r1-echo-service)

  (require '[net.jeffhui.atomix.log :as log] :reload)
  (def lg (log/distributed r1 {:protocol {:name        "log"
                                          :consistency :linearizable
                                          :replication :synchronous}}))

  (dotimes [i 100]
    (log/produce lg (format "hello world %d" i)))
  (def all-parts (log/partitions lg))
  (def outs (doall (map #(log/consume! % 0 (fn [m] (prn "RECV" m))) all-parts)))

  (def r-echo-service (run-service r1 "echo" {} (fn [msg] msg)))
  (def res (service-request r-echo-service {:hello "world"}))
  (def s1 (find-service r2 "echo"))
  (def res (service-request s1 {:hello "world2"}))
  (deref res 1000 :timeout)
  (.close r-echo-service)

  )
