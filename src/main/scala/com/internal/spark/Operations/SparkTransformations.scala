package com.internal.spark.Operations

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkTransformations {
  val sparkConf = new SparkConf().setAppName("Spark-Transformations").setMaster("local")
  val sc = new SparkContext(sparkConf)

  def main(args: Array[String]): Unit = {
    // map - apply f item by item
    map()

    // mapPartition - apply f in each partition; Get one item per partition
    mapPartitions()

    /* mapPartition - apply f in each partition; Get one item per partition
     * Retain partition index
     */
    mapPartitionsWithIndex()

    // filter - Apply f - Keep item if f returns TRUE
    filter()

    /* flatmap - applying a function to all elements of this RDD, and then flattening the results
     * It may create multiple items from single item of an RDD
     */
    flatMap()

    /*
     * Create a Pair RDD, forming one pair for each item in the original RDD
     */
    keyBy()

    /*
     * Group the data based on key - that f generates. Applied on any rdd
     */
    groupBy()

    /*
     * Group the data based on key - Applied on pair-rdd
     */
    groupByKey()

    /*
     * Group the data based on key - Grouped in each partition first; then shuffled
     * Applied on pair-rdd
     * 
     */
    reduceByKey()

    // Return a new RDD containing a statistical sample of the original RDD
    sample()

    /*
     * Unions both rdd
     * Retains partition info
     * Nerrow Transformation
     */
    union()

    /*
     * Joins based on key
     * Applies on pair-rdd
     */
    join()

    /*
     * distinct items from the original RDD (omitting all duplicates)
     */
    distinct()

    /* Return a new RDD which is reduced to a smaller number of partitions
     * Nerrow Transformation
     * # partitions can only be decreased
     */
    coalesce()

    /*
     * Return a new RDD with the specified number of partitions, placing original items into the partition
     * Specified by custom partitioner
     * On pair-rdd
     */
    partitionBy()

    /*
     * Return a new RDD containing pairs whose key is the item in the original RDD,
     * and whose value is that item’s corresponding element (same partition, same index) in a second RDD
     * Can only zip RDDs with same number of elements in each partition
     */
    zip()
  }

  def map() = {
    val x = sc.parallelize(Array(1, 2, 3, 4, 5))

    println("BEFORE")
    x.foreach(println)

    val y = x.map(x => (x, "MAP-FUNCTION APPLIED"))

    println("AFTER")
    y.foreach(println)
  }

  def mapPartitions() {
    // define # partitions as '2'
    val x = sc.parallelize(Array(1, 2, 3, 4, 5), 2)

    println("BEFORE")
    x.foreach(println)

    def f(i: Iterator[Int]) = { (i.sum, "MAP-PARTITION APPLIED").productIterator }

    val y = x.mapPartitions(f)

    println("AFTER")
    y.foreach(println)

  }

  def mapPartitionsWithIndex() {
    // define # partitions as '2'
    val x = sc.parallelize(Array(1, 2, 3, 4, 5), 2)

    println("BEFORE")
    x.foreach(println)

    def f(partitionIndex: Int, i: Iterator[Int]) = { (partitionIndex, i.sum).productIterator }

    val y = x.mapPartitionsWithIndex(f)

    println("AFTER")
    y.foreach(println)

  }

  def filter() {
    val x = sc.parallelize(Array(1, 2, 3, 4, 5))

    println("BEFORE")
    x.foreach(println)

    val y = x.filter(x => x % 2 == 0)

    println("AFTER")
    y.foreach(println)
  }

  def flatMap() = {
    val x = sc.parallelize(Array(1, 2, 3, 4, 5))

    println("BEFORE")
    x.foreach(println)

    val y = x.flatMap(x => Array(x, x * 100))

    println("AFTER")
    y.foreach(println)
  }

  def keyBy() {
    val x = sc.parallelize(Array("John", "Fred", "Anna", "James"))

    println("BEFORE")
    x.foreach(println)

    val y = x.keyBy(x => x.charAt(0))

    println("AFTER")
    y.foreach(println)
  }

  def groupBy() {
    val x = sc.parallelize(Array("John", "Fred", "Anna", "James"))

    println("BEFORE")
    x.foreach(println)

    val y = x.groupBy(x => x.charAt(0))

    println("AFTER")
    y.foreach(println)
  }

  def groupByKey() {
    val x = sc.parallelize(Array(("John", 100), ("Fred", 400), ("Anna", 200), ("John", 600)))

    println("BEFORE")
    x.foreach(println)

    val y = x.groupByKey()

    println("AFTER")
    y.foreach(println)
  }

  def reduceByKey() {
    val x = sc.parallelize(Array(("John", 100), ("Fred", 400), ("Anna", 200), ("John", 600)))

    println("BEFORE")
    x.foreach(println)

    val y = x.reduceByKey(_ + _)

    println("AFTER")
    y.foreach(println)
  }

  def sample() {
    val x = sc.parallelize(Array(1, 2, 3, 4, 5))

    println("BEFORE")
    x.foreach(println)

    /*
     * sample(withReplacement, fraction, seed=None)
     */
    val y = x.sample(false, 0.8)

    println("AFTER")
    y.foreach(println)
  }

  def union() {
    val x = sc.parallelize(Array(1, 2, 3), 2)
    val y = sc.parallelize(Array(3, 4), 1)
    val z = x.union(y)

    println("AFTER")
    z.foreach(println)
  }

  def join() {
    val x = sc.parallelize(Array(("A", 1), ("B", 2), ("C", 3)))
    val y = sc.parallelize(Array(("B", 4), ("C", 5), ("D", 6)))
    val z = x.join(y)

    println("AFTER")
    z.foreach(println)
  }

  def distinct() {
    val x = sc.parallelize(Array(1, 2, 3, 3, 1))

    println("BEFORE")
    x.foreach(println)

    val y = x.distinct()

    println("AFTER")
    y.foreach(println)
  }

  def coalesce() {
    val x = sc.parallelize(Array(1, 2, 3, 4, 5), 3)

    println("BEFORE")
    x.glom().collect().foreach(x => println(x.toList))

    val y = x.coalesce(4)

    println("AFTER")
    y.glom().collect().foreach(x => println(x.toList))
  }

  def partitionBy() {
    val x = sc.parallelize(Array("John", "Fred", "Anna", "James", "Rpal", "Kans"), 3)

    println("BEFORE")

    val y = x.map(x => (x.charAt(0), x))

    y.glom().collect().foreach(x => println(x.toList))

    import org.apache.spark.Partitioner

    val z = y.partitionBy(new Partitioner() {
      val numPartitions = 2
      def getPartition(k: Any) = {
        if (k.asInstanceOf[Char] < 'H') 0 else 1
      }
    })

    println("AFTER")
    z.glom().collect().foreach(x => println(x.toList))
  }

  def zip() {
    val x = sc.parallelize(Array(1, 2, 3))
    val y = sc.parallelize(Array(4, 9, 16))
    val z = x.zip(y)

    println("AFTER")
    z.foreach(println)
  }
}