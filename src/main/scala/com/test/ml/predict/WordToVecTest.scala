package com.test.ml.predict

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.mllib.feature.Word2VecModel

object WordToVecTest {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\winutils")

    val sc = new SparkConf().setAppName("Word2Vec").setMaster("local")
    val sparkContext = new SparkContext(sc)
    val sqlContext = new SQLContext(sparkContext)

    val input = sparkContext.textFile("C:\\Users\\Raktotpal\\Desktop\\sparkTest.txt")
      .map(line => line.split(",").toSeq)

    val word2vec = new Word2Vec()

    val model = word2vec.fit(input)

    val synonyms = model.findSynonyms("china", 40)

    for ((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    // Save and load model
    model.save(sparkContext, "myModelPath")
    val sameModel = Word2VecModel.load(sparkContext, "myModelPath")
  }

}