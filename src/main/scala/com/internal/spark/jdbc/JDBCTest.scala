package com.internal.spark.jdbc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

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
      "url" -> "jdbc:postgresql://dayrhectod004.enterprisenet.org:5432/intstudioDB?user=ndxis&password=ndx",
      "dbtable" -> dbtable));

    //jdbcDF.foreach(println)
  }

}