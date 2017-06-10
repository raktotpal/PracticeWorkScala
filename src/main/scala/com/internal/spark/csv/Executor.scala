package com.internal.spark.csv

import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType

//case class dummydata1(col1: String, col2: String, col3: String,
//        col4: String, col5: String, col6: String,
//        col7: String, col8: String)

//    case class dummydata(col1: String, col2: String, col3: String,
//        col4: Float, col5: Float, col6: Float,
//        col7: Integer, col8: Integer)

object Executor {
  def tryToInt(s: String): Int = { //Option[Int]
    try {
      s.toInt //Some(s.toInt)
    } catch {
      case e: Exception => 0 //None
    }
  }

  def tryToFloat(s: String): Float = { // Option[Float]
    try {
      s.toFloat
    } catch {
      case e: Exception => 0 //None
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SCALA-PARTITIONING").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // import sq.implicits._
    val myRowRDD = sc.textFile(args(0))
      .map(p => p.split(",", -1))
      .map(p => Row(p(0), p(1), p(2), tryToFloat(p(3)), tryToFloat(p(4)),
        tryToFloat(p(5)), tryToInt(p(6)), tryToInt(p((7)))))

    val schema = StructType(List(StructField("col1", StringType, true),
      StructField("col2", StringType, true),
      StructField("col3", StringType, true),
      StructField("col4", FloatType, true),
      StructField("col5", FloatType, true),
      StructField("col6", FloatType, true),
      StructField("col7", IntegerType, true),
      StructField("col8", IntegerType, true)))

    val mydf = sqlContext.createDataFrame(myRowRDD, schema)

    //        var mydf2 = mydf.sort($"col0".desc)

    import org.apache.spark.sql.functions._
    var mydf2 = mydf.sort(desc("col1"))
    mydf2.count()

    val result = queryDataUP(mydf2, 1, 48, 10)

    result.foreach(println)
  }

  //query data using partition
  def queryDataUP(df: DataFrame, page: Integer, start: Integer, limit: Integer): Array[Row] = {
    //page  parameter is redundant.. will not be used
    //var dfOrdered = df.orderBy(df.columns.head) //default ordering
    //Holder.log.info("queryData Params: " + page + "," + start + "," + limit)
    //var skipRecs = df.limit(start);
    val numpart = df.rdd.partitions.length
    val total = df.count

    val partsize = total / numpart
    var rows = new Array[Row](limit)
    val offset = start % partsize
    var i = 0 //to check if we have reached the offset within page
    var j = 0 //for page length
    val partindex = start / partsize

    // find how many records are in each partition and determine which partition to jump to
    println("partition index - " + partindex)
    println("offset - " + offset + "each partition size - " + partsize)

    def myfunc(index: Int, iter: Iterator[(Row)]): Iterator[Row] = {
      var a = Iterator[Row]()
      //work on only the required partitions, note that data could continue beyond partition boundary
      // more than 2 partition span is not supported
      if (index == partindex || index == partindex + 1)
        a = iter

      a
    }
    //df.rdd.mapPartitionsWithIndex(myfunc).collect

    for (r <- df.rdd.mapPartitionsWithIndex(myfunc).collect) {
      //start collecting after the offset, till our entire page length of data is collected
      if (i > offset && j < limit) {
        rows(j) = r
        j = j + 1
      }
      i = i + 1
    }

    rows
  }

}