package com.internal.spark.kryo

import org.apache.spark.{ SparkContext, SparkConf }
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.{ KryoSerializer, KryoRegistrator }

import scala.util.Random

class SparkWithKryo

class CustomKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    Console.err.println("################# MyRegistrator called")
    kryo.register(classOf[SparkWithKryo])
  }
}

class WrapperSerializer(conf: SparkConf) extends KryoSerializer(conf) {
  override def newKryo(): Kryo = {
    println("## Called newKryo!")
    super.newKryo()
  }
}

object SparkWithKryo {
  def main(args: Array[String]) {
    println("SparkDriver.main called")

    val conf = new SparkConf().setMaster("local")
    conf.set("spark.serializer", "com.internal.spark.kryo.WrapperSerializer")
    // "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer"
    conf.set("spark.kryo.registrator", "com.internal.spark.kryo.CustomKryoRegistrator")
    conf.set("spark.task.maxFailures", "1")

    println("We have a Spark config")

    val sc = new SparkContext(conf.setAppName("Spark with Kryo Serialisation"))

    println("We have a Spark context")

    val numElements = 10000

    // We cache this RDD to make sure the values are actually transported and not recomputed.
    val cachedRDD = sc.parallelize((0 until numElements).map((_, new SparkWithKryo)), 10).cache()
    
    println(s"cachedRDD.count() == " + cachedRDD.count())

    // Randomly mix the keys so that the join below will require a shuffle with each partition sending data to
    // many other partitions.
    val randomisedRDD = cachedRDD.map({ case (index, customObject) => (new Random().nextInt, customObject) })
    println(s"randomisedRDD.count() == " + randomisedRDD.count())

    // Join the two RDDs, and force evaluation.
    // val localResults = randomisedRDD.join(cachedRDD).collect()
    val localResults = randomisedRDD.collect()
    println(s"localResults.length() == " + localResults.length)
    println(s"localResults(${localResults.size}): ${localResults.mkString}")
  }
}