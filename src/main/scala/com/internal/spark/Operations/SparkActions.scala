package com.internal.spark.Operations

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkActions {
  val sparkConf = new SparkConf().setAppName("Spark-Transformations").setMaster("local")
  val sc = new SparkContext(sparkConf)

  def main(args: Array[String]): Unit = {
    // Get # of partitions in rdd
    getNumPartitions()

    // Return all items in the RDD to the driver in a single list
    collect()

    /* Aggregate all the elements of the RDD by applying a user function pairwise to elements and partial results,
     * and returns a result to the driver
     */
    reduce()

    /*
     * Aggregate all the elements of the RDD by:
     * -applying a user function to combine elements with user-supplied objects,
     * -then combining those user-defined results via a second user function,
     * -and finally returning a result to the driver
     */
    aggregrate()

    // max / sum / mean / stdev

    /*
     * Return a map of keys and counts of their occurrences in the RDD
     * On pair-rdd
     */
    countByKey()

    // saveastextfile

  }

  def getNumPartitions() = {
    val x = sc.parallelize(Array(1, 2, 3, 4, 5), 2)

    val y = x.getNumPartitions

    println("# partitions: " + y)
  }

  def collect() = {
    val x = sc.parallelize(Array(1, 2, 3, 4, 5))

    val y = x.collect

    y.foreach(println)
  }

  def reduce() = {
    val x = sc.parallelize(Array(1, 2, 3, 4, 5))

    val y = x.reduce((x, y) => x * y)

    println("REDUCE result: " + y)
  }

  def aggregrate() {
    def seqOp = (data: (List[Int], Int), item: Int) => (data._1 :+ item, data._2 + item)
    def combOp = (d1: (List[Int], Int), d2: (List[Int], Int)) => (d1._1.union(d2._1), d1._2 + d2._2)
    val x = sc.parallelize(List(1, 2, 3, 4, 5))
    val y = x.aggregate((List[Int](), 0))(seqOp, combOp)
    println(y)
  }

  def countByKey() {
    val x = sc.parallelize(Array("John", "Fred", "Anna", "James"))

    val y = x.map(x => (x.charAt(0), x))

    val z = y.countByKey()

    println("COUNTBYKEY: " + z)
  }
}