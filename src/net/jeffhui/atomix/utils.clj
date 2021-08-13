(ns net.jeffhui.atomix.utils
  (:require [clojure.string :as string])
  (:import [io.atomix.cluster MemberId Node]
           [io.atomix.utils.serializer Serializer Namespace]
           [com.esotericsoftware.kryo Kryo]
           [com.esotericsoftware.kryo.io Output Input]
           java.util.UUID
           java.util.function.BiFunction
           java.util.function.Function
           java.util.function.Consumer
           java.util.function.Predicate
           java.time.Duration
           java.time.temporal.ChronoUnit))

(defn ^Duration ->duration
  ([v]
   (if (instance? Duration v)
     v
     (if (or (vector? v) (list? v))
       (->duration (first v) (second v))
       (->duration (long v) :seconds))))
  ([amount unit] (Duration/of (long amount) (ChronoUnit/valueOf (string/upper-case (name unit))))))

(defn ^Function func [f]
  (if (instance? Function f)
    f
    (reify Function
      (apply [_ t] (f t)))))

(defn ^BiFunction bifunc [f]
  (if (instance? BiFunction f)
    f
    (reify BiFunction
      (apply [_ t u] (f t u)))))

(defn ^Predicate predicate [f]
  (if (instance? Predicate f)
    f
    (reify Predicate
      (test [_ t] (f t)))))

(defn ^Consumer consumer [f]
  (if (instance? Consumer f)
    f
    (reify Consumer
      (accept [_ t] (f t)))))

(defn- coll-serializer [empty-coll]
  (proxy [com.esotericsoftware.kryo.Serializer] []
    (write [kryo ^Output output coll]
      (.writeInt output (count coll))
      (doseq [item coll]
        (.writeClassAndObject ^Kryo kryo output item)))
    (read [^Kryo kryo ^Input input klass]
      (loop [n    (.readInt input)
             coll (transient empty-coll)]
        (if (pos? n)
          (recur (dec n) (conj! coll (.readClassAndObject ^Kryo kryo input)))
          (persistent! coll))))))

(defn- map-serializer [empty-coll]
  (proxy [com.esotericsoftware.kryo.Serializer] []
    (write [kryo ^Output output coll]
      (.writeInt output (count coll))
      (doseq [[k v] coll]
        (.writeClassAndObject ^Kryo kryo output k)
        (.writeClassAndObject ^Kryo kryo output v)))
    (read [^Kryo kryo ^Input input klass]
      (loop [n    (.readInt input)
             coll (transient empty-coll)]
        (if (pos? n)
          (recur (dec n) (assoc! coll
                                 (.readClassAndObject ^Kryo kryo input)
                                 (.readClassAndObject ^Kryo kryo input)))
          (persistent! coll))))))

(def clojure-namespace-serializer
  (-> (Namespace/builder)
      (.register (proxy [com.esotericsoftware.kryo.Serializer] []
                   (write [kryo ^Output output object]
                     (.writeString output (str (.-sym ^clojure.lang.Keyword object))))
                   (read [kryo ^Input input klass]
                     (clojure.lang.Keyword/intern (.readString input))))
                 (into-array Class [clojure.lang.Keyword]))
      (.register (proxy [com.esotericsoftware.kryo.Serializer] []
                   (write [kryo ^Output output object]
                     (.writeString output (str object)))
                   (read [kryo ^Input input klass]
                     (symbol (.readString input))))
                 (into-array Class [clojure.lang.Symbol]))
      (.register (proxy [com.esotericsoftware.kryo.Serializer] []
                   (write [kryo ^Output output object]
                     (.writeLong output (.getMostSignificantBits ^UUID object) false)
                     (.writeLong output (.getLeastSignificantBits ^UUID object) false))
                   (read [kryo ^Input input klass]
                     (UUID. (.readLong input false) (.readLong input true))))
                 (into-array Class [java.util.UUID]))
      (.register (coll-serializer []) (into-array Class [clojure.lang.PersistentVector]))
      (.register (coll-serializer '()) (into-array Class [clojure.lang.PersistentList]))
      (.register (coll-serializer #{}) (into-array Class [clojure.lang.PersistentHashSet]))
      (.register (coll-serializer []) (into-array Class [clojure.lang.LazySeq]))
      (.register (map-serializer {}) (into-array Class [clojure.lang.PersistentHashMap]))
      (.register (map-serializer {}) (into-array Class [clojure.lang.PersistentArrayMap]))
      (.build)))

(def clojure-serializer (Serializer/using clojure-namespace-serializer))
(def ^:dynamic ^Serializer *default-serializer* clojure-serializer)

(defn ^MemberId ->member-id [id]
  (cond
    (instance? MemberId id) id
    (instance? Node id) (.id ^Node id)
    :else (MemberId/from (str id))))
