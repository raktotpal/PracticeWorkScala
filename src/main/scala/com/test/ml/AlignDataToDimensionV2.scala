package com.test.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import scala.util.control.Breaks._
import org.mapdb.DBMaker
import scala.collection.Map
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import java.io.PrintWriter
import java.io.File

/**
 *
 * This module aligns the input data to particular dimensions.
 * Takes the Header line information from dataset which gives the Column name information.
 * Use Bag of Words (One to One mapping) and align to particular dimension.
 *
 * Usage:
 * ./spark-submit --class com.ndx.integrationStudio.AlignDataToDimension --jars $<dependent jars> $JAR_LOCATION <args list>
 *
 * @args
 * args[0] => File Path for the Train-Data in One-to-One mapping with Dimension form
 * args[1] => File Path for the input test data set to be aligned.
 * args[2] => File path for output
 *
 * @author Raktotpal Bordoloi
 *
 */
object AlignDataToDimensionV2 {
  val sc = new SparkConf().setAppName("Align-Dimensions").setMaster("local")
  val sparkContext = new SparkContext(sc)
  val sqlContext = new SQLContext(sparkContext)

  val delimiterChar = ","

  def main(args: Array[String]): Unit = {
    val data = sparkContext.textFile("E:\\RPAL\\KproZ\\NIELSEN_INTEGRATION_STUDIO\\data\\export.csv")
    val bagOfWords = data.map(s => s.split(",")).map(s => (s(0), s(1)))

    val headerLine = extractHeaderFromDataSet(args)

    val headerSet = headerLine.split(delimiterChar)

    val appendingHeader: StringBuffer = new StringBuffer

    val pw = new PrintWriter(new File("E:\\RPAL\\KproZ\\NIELSEN_INTEGRATION_STUDIO\\data\\GUM360NGA\\class_predictions.txt"))

    for (eachColumnName <- headerSet) {

      // Go for Cosine similarities

      val scores = bagOfWords.map(x => (x._2, consineTextSimilarity(x._2, eachColumnName)))
      val maxKey = scores.takeOrdered(1)(Ordering[Double].reverse.on(_._2))(0)._1
      var classlabel = bagOfWords.map(x => (x._2, x._1)).lookup(maxKey).head

      /*
                 * If No closer match found, it selects the TOP/FIRST key from the bag.
                 */
      classlabel = if (classlabel.equalsIgnoreCase("DIM_NAME")) "MISC" else classlabel

      pw.write(eachColumnName + "," + classlabel + "\n")
    }

    pw.close()
  }

  def extractHeaderFromDataSet(args: Array[String]): String = {
    //        val testRDD = sparkContext.textFile(args(1))

    val testRDD = sparkContext.textFile("E:\\RPAL\\KproZ\\NIELSEN_INTEGRATION_STUDIO\\data\\GUM360NGA\\GUM360NGA.csv")

    val headerLine = testRDD.take(1).head

    return headerLine
  }

  /**
   * The resulting similarity ranges from −1 meaning exactly opposite, to 1
   * meaning exactly the same, with 0 usually indicating independence, and
   * in-between values indicating intermediate similarity or dissimilarity.
   *
   * For text matching, the attribute vectors A and B are usually the term
   * frequency vectors of the documents. The cosine similarity can be seen as
   * a method of normalizing document length during comparison.
   *
   * In the case of information retrieval, the cosine similarity of two
   * documents will range from 0 to 1, since the term frequencies (tf-idf
   * weights) cannot be negative. The angle between two term frequency vectors
   * cannot be greater than 90°.
   *
   * @param leftVector
   * @param rightVector
   * @return
   */
  def consineTextSimilarity(left1: String, right1: String): Double = {
    val left = left1.toLowerCase().split("[_/ \\.]")
    val right = right1.toLowerCase().split("[_/ \\.]")
    // left: Array[String], right: Array[String]

    val leftWordCountMap = new HashMap[String, Integer]();
    val rightWordCountMap = new HashMap[String, Integer]();
    val uniqueSet = new HashSet[String]();
    var temp: Integer = null;
    for (leftWord <- left) {
      temp = leftWordCountMap.getOrElse(leftWord, 0)
      if (temp == 0) {
        leftWordCountMap.put(leftWord, 1);
        uniqueSet.add(leftWord);
      } else {
        leftWordCountMap.put(leftWord, temp + 1);
      }
    }
    for (rightWord <- right) {
      temp = rightWordCountMap.getOrElse(rightWord, 0)
      if (temp == 0) {
        rightWordCountMap.put(rightWord, 1);
        uniqueSet.add(rightWord);
      } else {
        rightWordCountMap.put(rightWord, temp + 1);
      }
    }
    val leftVector = new Array[Integer](uniqueSet.size) //int[uniqueSet.size];

    val rightVector = new Array[Integer](uniqueSet.size) //int[uniqueSet.size()];
    var index = 0
    var tempCount: Integer = null
    for (uniqueWord <- uniqueSet) {
      tempCount = leftWordCountMap.getOrElse(uniqueWord, 0)
      leftVector(index) = tempCount // == null ? 0: tempCount
      tempCount = rightWordCountMap.getOrElse(uniqueWord, 0)
      rightVector(index) = tempCount // == null ? 0: tempCount
      index = index + 1
    }
    return consineTextSimilarity(leftVector, rightVector);
  }

  def consineTextSimilarity(leftVector: Array[Integer], rightVector: Array[Integer]): Double = {
    if (leftVector.length != rightVector.length)
      return 1;
    var dotProduct = 0.0;
    var leftNorm = 0.0;
    var rightNorm = 0.0;
    for (i <- 0 until leftVector.length) {
      dotProduct = dotProduct + leftVector(i) * rightVector(i)
      leftNorm = leftNorm + leftVector(i) * leftVector(i)
      rightNorm = rightNorm + rightVector(i) * rightVector(i)
    }

    val result = dotProduct / (Math.sqrt(leftNorm) * Math.sqrt(rightNorm));
    return result;
  }
}