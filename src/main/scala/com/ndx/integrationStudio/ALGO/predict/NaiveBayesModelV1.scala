package com.ndx.integrationStudio.ALGO.predict

import org.apache.spark.mllib.feature.{ HashingTF, IDF }
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD

/*
 * This implementation is of NaiveBayes Model
 * having Train data labeled
 * and 
 * trying test data with different label
 * to identify best  suited label or category
 * 
 */

object NaiveBayesModelV1 {

  def tokenize(line: String): Seq[String] = {
    val regex = """[^0-9]*""".r
    val stopwords = Set(
      "the", "a", "an", "of", "or", "in", "for", "by", "on", "but", "is", "not",
      "with", "as", "was", "if",
      "they", "are", "this", "and", "it", "have", "from", "at", "my",
      "be", "that", "to")

    line.split(",")
      .map(_.toLowerCase)
      .filter(token => regex.pattern.matcher(token).matches)
      .filterNot(token => stopwords.contains(token))
      .filter(token => token.size >= 2)
      .toSeq
  }

  def getLabeled(data: RDD[String], label: Double): RDD[LabeledPoint] = {

    case class TestLabeledData(label: Double, text: String)

    val raw = data.map { row =>
      TestLabeledData(label, row)
    }

    val dim = math.pow(2, 18).toInt
    val tokenizedData = raw.map {
      case TestLabeledData(label, text) => (label, tokenize(text))
    }

   /* val hashingTF = new HashingTF(dim)
    val tf = tokenizedData.map { case (label, text) => (label, hashingTF.transform(text)) }

    val idf = new IDF().fit(tf.map(_._2))

    val labbeledRDD = tf.map {
      case (label, rawFeatures) => LabeledPoint(label, idf.transform(rawFeatures))
    }*/
    
    val hashingTF = new HashingTF(dim)
    val labbeledRDD = tokenizedData.map { case (label, text) => LabeledPoint(label, hashingTF.transform(text)) }


    labbeledRDD.foreach(println)
    (labbeledRDD)
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("FeatureExtractor").setMaster("local")

    val sc = new SparkContext(sparkConf)

    /* val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._*/

    case class InputData(category: String, text: String)
    case class LabeledData(category: String, text: String, label: Double)

    val trainData = sc.textFile("F:\\train.txt")
    val testData = sc.textFile("F:\\test.txt")

    val inputData = trainData.map { row =>
      val token = row.split(";")
      val ctgry = token(0)
      val text = token(1)

      InputData(ctgry, text)
    }

    val categoryMap = inputData.map(x => x.category).distinct.zipWithIndex.mapValues(x => x.toDouble).collectAsMap

    val labeledData = inputData.map(x => LabeledData(x.category, x.text, categoryMap.get(x.category).getOrElse(0.0)))

    //val df  = sqlContext.createDataFrame(labeledData).toDF("category", "text", "label")

    val dim = math.pow(2, 18).toInt

    val raw = labeledData.map {
      case LabeledData(catagory, text, label) => (label, tokenize(text))
    }

/*  val hashingTF = new HashingTF(dim)
    val tf = raw.map { case (label, text) => (label, hashingTF.transform(text)) }

    val idf = new IDF().fit(tf.map(_._2))

    val labbeledRDD = tf.map {
      case (label, rawFeatures) => LabeledPoint(label.toInt, idf.transform(rawFeatures))
    }
*/

    val hashingTF = new HashingTF(dim)
    val labbeledRDD = raw.map { case (label, text) => LabeledPoint(label.toInt, hashingTF.transform(text)) }
    
    
    // defining model

    val model = NaiveBayes.train(labbeledRDD, lambda = 1.0, modelType = "multinomial")

    // testing for all category

    val predictionResult = scala.collection.mutable.Map[String, Double]()

    for (category <- categoryMap) {
      val testLabeledData = getLabeled(testData, category._2)

      val predictionAndLabel = testLabeledData.map(p => (model.predict(p.features), p.label))
      val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testData.count()

      predictionResult += (category._1 -> accuracy)

      println("Category : " + category._1 + "\n" +
        "Category Index : " + category._2 + "\n" +
        "Accuraccy : " + accuracy)
    }

    predictionResult.foreach(println)
  }
}