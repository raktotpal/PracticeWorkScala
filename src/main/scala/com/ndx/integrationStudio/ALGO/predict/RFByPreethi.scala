package com.ndx.integrationStudio.ALGO.predict

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

object RFByPreethi {
    val conf = new SparkConf().setAppName("Spark RandomForest").setMaster("local")
    val sc = new SparkContext(conf)
    
    /**
     * Created by preethi_sankaranarayanan on 12/6/15.
     */
    def retrieveindex(stringlist: List[String], lookuplist: List[String]) =
        stringlist.map(y => lookuplist.indexOf(y).toString + ":1").mkString(" ")

    def main(args: Array[String]) {
        val data = sc.textFile("E:\\RPAL\\KproZ\\NIELSEN_INTEGRATION_STUDIO\\data\\train.txt")
        val distinctdata = data.map(s => (s.split("[,;]").toSet.toArray.head, s.split("[,;]").toSet.toArray.tail))
        //distinctdata.take(1).foreach{ x => println(x.deep.mkString("\n"))}
        val labels = distinctdata.map(x => x._1).zipWithIndex()
        //labels.take(1).foreach{ x => println(x)}
        val features = distinctdata.map(x => x._2)
        //features.take(1).foreach{ x => println(x.deep.mkString("\n"))}
        val featureSet = features.map(x => (1, x.mkString(" "))).groupByKey().map(x => x._2.mkString(" ")).collect().take(1).deep.mkString(" ").split(' ')
        val libsvmlabel = distinctdata.join(labels)
        //use libsvm to write to a file in libsvm format
        val libsvm = libsvmlabel.map(x => x._2._2.toString() + retrieveindex(x._2._1.toList, featureSet.toList))

        //println(featureSet.indexOf("OfficeFurnishings"))
        libsvm.take(1).foreach { x => println(x.mkString(" ")) }
        //libsvmlabel.take(1).foreach{x=>println(x._2._1.mkString(" "))}

        def indicesretrieve(stringlist: List[String], lookuplist: List[String]) =
            stringlist.map(y => (lookuplist.indexOf(y), 1.0)).toSeq

        //create rdd of labeledpoint
        val data1 = libsvmlabel.map(x => LabeledPoint(x._2._2,
            Vectors.sparse(featureSet.toList.length, indicesretrieve(x._2._1.toList, featureSet.toList))))
        // Split the data into training and test sets (30% held out for testing)
        val splits = data1.randomSplit(Array(0.7, 0.3))
        val (trainingData, testData) = (splits(0), splits(1))

        // Train a RandomForest model.
        //  Empty categoricalFeaturesInfo indicates all features are continuous.
        val numClasses = labels.count().toInt
        val categoricalFeaturesInfo = Map[Int, Int]()
        val numTrees = 80 // Use more in practice.
        val featureSubsetStrategy = "auto" // Let the algorithm choose.
        val impurity = "gini"
        val maxDepth = 4
        val maxBins = 32

        val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
            numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

        // Evaluate model on test instances and compute test error
        val labelAndPreds = testData.map { point =>
            val prediction = model.predict(point.features)
            (point.label, prediction)
        }
        val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
        println("Test Error = " + testErr)
        println("Learned classification forest model:\n" + model.toDebugString)

        // Save and load model
        model.save(sc, "myModelPath")
        val sameModel = RandomForestModel.load(sc, "myModelPath")
    }

}