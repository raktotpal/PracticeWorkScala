package com.test.ml.predict

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag.Nothing

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SQLContext

object Prototype1 {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\winutils")

    val sc = new SparkConf().setAppName("Test-N-Gram").setMaster("local")
    val sparkContext = new SparkContext(sc)
    val sqlContext = new SQLContext(sparkContext)

    //val trainedRDD = sparkContext.textFile("C:\\Users\\Raktotpal\\Desktop\\TrainData.txt").toArray

    val testRDD = sparkContext.textFile("C:\\Users\\Raktotpal\\Desktop\\testMyData.txt").collect()

    val seqObj = Seq()

    seqObj :+ ("col2", testRDD(1).split(" "))

    val wordDataFrame = sqlContext.createDataFrame(seqObj).toDF("label", "words")

    //        val seq = Seq()
    //        
    //        var count = 1
    //        for (i <- testRDD) {
    //            println("---------------> " + i)
    //            seq :+  ("col"+ count, i.split(" "))
    //            
    //            count += 1
    //        }

    //        val wordDataFrame = sqlContext.createDataFrame(seq).toDF("label", "words")

    val ngram = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams")
    val ngramDataFrame = ngram.transform(wordDataFrame)

    val nGrammedRDD = ngramDataFrame.rdd
    //.map(_.getAs[Stream[String]]("ngrams").toList)

    val vv = Seq(
      ("1", nGrammedRDD.first()(0)),
      ("2", nGrammedRDD.first()(1)),
      ("3", nGrammedRDD.first()(2)),
      ("4", nGrammedRDD.first()(3)),
      ("5", nGrammedRDD.first()(4)),
      ("6", nGrammedRDD.first()(5)))

    val dataFrameObj = sqlContext.createDataFrame(Seq(
      ("1", nGrammedRDD.first()(0)),
      ("2", nGrammedRDD.first()(1)),
      ("3", nGrammedRDD.first()(2)),
      ("4", nGrammedRDD.first()(3)),
      ("5", nGrammedRDD.first()(4)),
      ("6", nGrammedRDD.first()(5)))).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(dataFrameObj)
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(2)
    val featurizedData = hashingTF.transform(wordsData)

    // featurizedData.printSchema

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    rescaledData.printSchema
    rescaledData.select("label", "sentence", "words", "features").foreach(x => println(x))

  }

}