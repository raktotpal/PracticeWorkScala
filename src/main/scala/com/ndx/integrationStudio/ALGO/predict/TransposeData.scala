package com.ndx.integrationStudio.ALGO.predict

/**
 * Usage :-
 *
 *   spark-submit \
 *   --master <local or cluster or client> \
 *   --class com.nielsen.ndx.TransposeData \
 *   <ApplicationJar file Path>
 *   <Arguments for application
 *
 * 		args(0) => File path of test/train Data as input
 *    args(1) => File path of corresponding LIB SVM File as output
 *    args(2) => File path to store bagOfWords from Training Data
 *    args(3) => String value (train : in case Training Data as input
 *    													test  : in case Test Data as input)
 * 		>
 *
 */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
//import com.typesafe.config.ConfigException.Parse
import scala.util.Random
import scala.collection.mutable.StringBuilder

object TransposeData {

  def getTransposed(data: RDD[String], header: Array[String]): RDD[(Int, Iterable[String])] = {

    val transposed = data.flatMap(_.split(",").zipWithIndex).groupBy(_._2).mapValues(_.map(_._1))

    //transposed.foreach(println)

    transposed
  }

  def getLibSVM(transposed: RDD[(Int, Iterable[String])], featureindex: Map[String, Int]): RDD[String] = {

    val libsvm = transposed.map { row =>

      val label = row._1.toString
      var feature = ""

      var index = List[Int]()

      for (word <- (row._2.toArray.distinct)) {

        var value = Random.nextInt(99999)

        if (featureindex.get(word) == None)
          index ::= value
        else
          index ::= featureindex.get(word).get + 1
      }
      index = index.toList.sorted

      for (word <- index) {
        feature = feature + " " + word + ":1"
      }
      (label + feature)
    }

    libsvm.foreach { println }
    libsvm
  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("FeatureExtractor").setMaster("local")

    val sc = new SparkContext(sparkConf)

    val trainFilePath = args(0)
    val trainLibSVMFilePath = args(1)
    val bagOfWordsFilePath = args(2)
    val inputDataSetValue = args(3)

    // working with Train Data to extract features

    // Creating BagOfWord of unique word and corresponding Index 

    var bagOfWords = Map[String, Int]()

    if (inputDataSetValue.trim().toLowerCase() == "test") {

      bagOfWords = sc.textFile(bagOfWordsFilePath)
        .map { row =>
          val splitted = row.split(",")
          (splitted(0).toString(), splitted(1).toInt)
        }.collect().toMap

    } else if (inputDataSetValue.trim().toLowerCase() == "train") {
      bagOfWords = (sc.wholeTextFiles(trainFilePath)).flatMap(data =>
        (data._2.split("[,\r\n]").distinct).sorted)
        .zipWithIndex
        .map(x => (x._1, x._2.toInt))
        .collect()
        .toMap

      var all = List[String]()
      for (elem <- bagOfWords) {
        all ::= (elem._1 + "," + elem._2.toString())
      }
      val bagOfWordsSeq = sc.parallelize(all)
      bagOfWordsSeq.foreach { println }
      bagOfWordsSeq.saveAsTextFile(bagOfWordsFilePath)
    }

    //Reading CSV train data set 
    val trainData = sc.textFile(trainFilePath)

    // getting Header from Data
    val header = trainData.first.split(",").map(word => word.trim)

    // Getting data transposed from Column to row
    val transposed = getTransposed(trainData, header)

    // Getting LibSVM file generated for train data
    val trainLibSVM = getLibSVM(transposed, bagOfWords)

    trainLibSVM.saveAsTextFile(trainLibSVMFilePath)

  }
}