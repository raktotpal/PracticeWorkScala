package com.internal.spark.jsonAvro

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf

object AvroDf {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SchemaInfer").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val finalData = sqlContext.read.json("D:\\Danamon_NTP_sample.json")
    finalData.printSchema()
    import com.databricks.spark.avro._
    finalData.write.avro("D:\\aaaa.avro")

  }
}