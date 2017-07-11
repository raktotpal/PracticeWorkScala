package com.internal.spark.jdbc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

object JDBCTest {
  val classes = Seq(
    getClass,
    classOf[org.postgresql.Driver])
  val jars = classes.map(_.getProtectionDomain().getCodeSource().getLocation().getPath())

  val conf = new SparkConf().setAppName("FILTER-UTILITY").setMaster("local").setJars(jars)
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val dbtable = "intstudio_dev1.project_input_characteristics_t"

  def main(args: Array[String]) {
    val jdbcDF = sqlContext.load("jdbc", Map(
      "url" -> "jdbc:postgresql://XXXX:5432/intstudioDB?user=user&password=pass",
      "dbtable" -> dbtable));

//    import org.apache.spark.sql.functions._
//    val ageInINT = udf { (make: String) =>
//      Integer.parseInt(make.substring(1))
//    }
//    jdbcDF.withColumn("age", ageInINT(jdbcDF("age"))).show
  }

}