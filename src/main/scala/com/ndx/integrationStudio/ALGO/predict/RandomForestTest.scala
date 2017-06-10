package com.ndx.integrationStudio.ALGO.predict

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object RandomForestTest {
  def main(args: Array[String]) {
    val sc = new SparkConf().setAppName("Test-N-Gram").setMaster("local")
    val sparkContext = new SparkContext(sc)
    val sqlContext = new SQLContext(sparkContext)

    // Load and parse the data file.
    val data =
      MLUtils.loadLabeledData(sparkContext, "C:\\Users\\Raktotpal\\Desktop\\TrainData.txt")
    // Split data into training/test sets
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    val treeStrategy = Strategy.defaultStrategy("Classification")
    val numTrees = 3 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val model = RandomForest.trainClassifier(trainingData,
      treeStrategy, numTrees, featureSubsetStrategy, seed = 12345)

    // Evaluate model on test instances and compute test error
    val testErr = testData.map { point =>
      val prediction = model.predict(point.features)
      if (point.label == prediction) 1.0 else 0.0
    }.mean()
    println("Test Error = " + testErr)
    println("Learned Random Forest:\n" + model.toDebugString)
  }
}