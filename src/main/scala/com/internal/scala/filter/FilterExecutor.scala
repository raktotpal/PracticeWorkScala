package com.internal.scala.filter

import org.apache.spark.sql.DataFrame
import com.internal.scala.partition.ParquetUtility
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

object FilterExecutor {
    val conf = new SparkConf().setAppName("FILTER-UTILITY").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    def main(args: Array[String]) {
        var parsedDF: DataFrame = null

        val inputPath = args(0).trim()

        parsedDF = ParquetUtility.parseCSV(sqlContext, inputPath, true, ",", true)
        
        parsedDF.printSchema
        
        val x = parsedDF.rdd
        
        val y = x.map(x=> Row(x.get(1), x.get(2)))

    }
}