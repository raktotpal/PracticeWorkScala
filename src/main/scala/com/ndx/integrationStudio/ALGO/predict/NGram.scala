package com.ndx.integrationStudio.ALGO.predict

import scala.Array.canBuildFrom
import scala.reflect.runtime.universe
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature._

object NGram {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "E:\\winutils")

    val sc = new SparkConf().setAppName("Test-N-Gram").setMaster("local")
    val sparkContext = new SparkContext(sc)
    val sqlContext = new SQLContext(sparkContext)

    val arrayRDD = sparkContext.textFile("C:\\Users\\Raktotpal\\Desktop\\sparkTest.txt").collect()

    //        val wordDataFrame = sqlContext.createDataFrame(Seq(
    //            (0, Array("Hi", "I", "heard", "about", "Spark")),
    //            (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
    //            (2, Array("Logistic", "regression", "models", "are", "neat")))).toDF("label", "words")
    val seqObjs = Seq(
      ("col1", arrayRDD(0).split(",")),
      ("col2", arrayRDD(1).split(",")),
      ("col3", arrayRDD(2).split(",")),
      ("col4", arrayRDD(3).split(",")))

    val wordDataFrame = sqlContext.createDataFrame(seqObjs).toDF("label", "words")

    val ngram = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams")
    val ngramDataFrame = ngram.transform(wordDataFrame)

    //ngramDataFrame.map( x => x.getAs[Stream[String]]("ngrams")).foreach(println)
  }
}