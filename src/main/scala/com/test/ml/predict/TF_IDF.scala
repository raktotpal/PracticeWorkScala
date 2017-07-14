package com.test.ml.predict

import org.apache.spark.ml.feature.{ HashingTF, IDF, Tokenizer }
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

object TestNGram {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "E:\\winutils")

    val sc = new SparkConf().setAppName("Test-N-Gram").setMaster("local")
    val sparkContext = new SparkContext(sc)
    val sqlContext = new SQLContext(sparkContext)

    val arrayRDD = sparkContext.textFile("C:\\Users\\Raktotpal\\Desktop\\DimensionsData.txt").collect()

    val vv = ("Customer Name", arrayRDD(0))
    val v = Seq(
      ("Customer Name", arrayRDD(0)),
      ("Province", arrayRDD(1)),
      ("Product Category", arrayRDD(2)),
      ("Order Date", arrayRDD(3)))

    val dataFrameObj = sqlContext.createDataFrame(Seq(
      ("Customer Name", arrayRDD(0)),
      ("Province", arrayRDD(1)),
      ("Product Category", arrayRDD(2)),
      ("Order Date", arrayRDD(3)))).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(dataFrameObj)
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hashingTF.transform(wordsData)

    featurizedData.select("rawFeatures", "label").foreach(x => println(x))

    //        val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    //        val idfModel = idf.fit(featurizedData)
    //        val rescaledData = idfModel.transform(featurizedData)
    //        rescaledData.select("features", "label").take(3).foreach(println)

  }
}