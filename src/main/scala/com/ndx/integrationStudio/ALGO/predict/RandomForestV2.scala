package com.ndx.integrationStudio.ALGO.predict

/**
 * Usage :-
 *
 *   spark-submit \
 *   --master <local or cluster or client> \
 *   --class com.nielsen.ndx.RandomForestV2 \
 *   <ApplicationJar file Path>
 *   <Arguments for application
 *
 * 		args(0) => LibSVM File path of Train Data as input
 *    args(1) => LibSVM File path of Test Dataas output
 *    args(2) => File path of bagOfWords from Training Data
 *
 * 		>
 *
 */
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.feature.{ HashingTF, IDF }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import com.codahale.metrics.Metric

object RandomForestV2 {

  def trainModel(labeledTrainData: RDD[LabeledPoint]): RandomForestModel = {

    val numClasses = labeledTrainData.count().toInt
    val catFetInfo = new scala.collection.immutable.HashMap[Int, Int]
    // Train a RandomForest model.
    val treeStrategy = Strategy.defaultStrategy("Classification")
    val numTrees = 6 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val model = RandomForest.trainClassifier(labeledTrainData, numClasses, catFetInfo,
      numTrees, featureSubsetStrategy, impurity = "gini", maxDepth = 5, maxBins = 32, seed = 12345)
    (model)
  }

  def predictModel(labeledTestData: RDD[LabeledPoint], model: RandomForestModel): RDD[(Double, Double)] = {

    val predictions = labeledTestData.map(p => {
      val prediction = model.predict(p.features)
      (p.label, prediction)
    })

    predictions.foreach(println)
    println("*** Done ***")
    predictions
  }

  def getCategory(testData: RDD[LabeledPoint], bagOfWords: Map[String, Int], predictions: Map[Double, Double]): RDD[(Double, String)] = {

    val predictedLabels = testData.map { row =>
      val labelOfTestFile = row.label
      val predictionsValue = predictions.get(labelOfTestFile).get

      val categoryOfTestFile = bagOfWords.filter(elem => elem._2 == (predictionsValue - 1).toInt).toList //.map(elem => elem._1).toList

      (labelOfTestFile, categoryOfTestFile.toString)
    }
    predictedLabels
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("FeatureExtractor").setMaster("local")

    val sc = new SparkContext(sparkConf)

    val trainLibSVMFilePath = args(0)
    val testLibSVMFilePath = args(1)
    val bagOfWordFilePath = args(2)

    val trainingData = MLUtils.loadLibSVMFile(sc, trainLibSVMFilePath)
    val testData = MLUtils.loadLibSVMFile(sc, testLibSVMFilePath)

    val bagOfWords = sc.textFile(bagOfWordFilePath).map { row =>
      val splitted = row.split(",")
      (splitted(0), splitted(1).toInt)
    }.collect().toMap

    val model = trainModel(trainingData)

    val predictions = predictModel(testData, model).collect().toMap

    val predictedLabels = getCategory(testData, bagOfWords, predictions)

    model.toDebugString
    predictedLabels.foreach(println)
  }
}