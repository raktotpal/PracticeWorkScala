package com.ndx.integrationStudio.ALGO.predict

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.ml.feature._
import scala.collection.Seq

object Prototype4 {

    def main(args: Array[String]): Unit = {
        val sc = new SparkConf().setAppName("Test").setMaster("local")
        val sparkContext = new SparkContext(sc)
        val sqlContext = new SQLContext(sparkContext)

        System.setProperty("hadoop.home.dir", "E:\\winutils")

        val numClusters = 3
        val numIterations = 30

        val outputModelDir = "C:\\Users\\Raktotpal\\Desktop\\MyOutPut"

        /*
         * the sun is bright
         * the sky is blue
         */
        val trainData = sparkContext.textFile("C:\\Users\\Raktotpal\\Desktop\\TrainData.txt", 1)
        		//.textFile("E:\\RPAL\\KproZ\\NIELSEN_INTEGRATION_STUDIO\\data\\GUM360NGA_trained", 1)

        /*
         * the sun in the sky is bright
         */
        val testData = sparkContext.textFile("C:\\Users\\Raktotpal\\Desktop\\testMyData.txt", 1)
        		//.textFile("E:\\RPAL\\KproZ\\NIELSEN_INTEGRATION_STUDIO\\data\\MRS360NGA_TEST", 1)

        val vectors = trainData.map(x => featurize(x)).cache

        vectors.count() // Calls an action to create the cache.

        val model = KMeans.train(vectors, numClusters, numIterations)
        //        sparkContext.makeRDD(model.clusterCenters, numClusters).saveAsTextFile(outputModelDir)

        for (i <- 0 until numClusters) {
            println(s"\nCLUSTER $i:")
            testData.foreach { t =>
                if (model.predict(featurize(t)) == i) {
                    println(t)
                }
            }
        }
    }

    def featurize(s: String): Vector = {
        val numFeatures = 3

        val tf = new HashingTF(numFeatures)
        val c = tf.transform(s.sliding(2).toSeq)

        return c
    }

}