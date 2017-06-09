package com.internal.scala.tricky

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object ProblemStatement {
    def main(args: Array[String]) {
        val master = "local"
        val appName = "testing"
        var sc: SparkContext = null
        var sqlContext: SQLContext = null

        println("Insiatiating BaseSpec: SparkConf.................................")
        val conf = new SparkConf()
            .setMaster(master)
            .setAppName(appName)
            .set("spark.driver.allowMultipleContexts", "true")

        sc = new SparkContext(conf)
        sqlContext = new SQLContext(sc)

        var x: RDD[_] = null
        x = sc.parallelize(Array(0, 1, 2, 3, 4, 5))
        var y = sc.parallelize(Array(100, 101, 102, 103, 104, 105))

        for (temp <- 1 to 5) {
            x = x.zip(y) // .collect.foreach(println)
        }

        x.foreach(println)
    }
}