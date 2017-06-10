package com.ndx.integrationStudio

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import scala.util.control.Breaks._
import org.mapdb.DBMaker
import scala.collection.Map
import scala.collection.mutable.HashMap

/**
 *
 * This module aligns the input data to particular dimensions.
 * Takes the Header line information from dataset which gives the Column name information.
 * Use Bag of Words (One to One mapping) and align to particular dimension.
 *
 * Usage:
 * ./spark-submit --class com.ndx.integrationStudio.AlignDataToDimension --jars $<dependent jars> $JAR_LOCATION
 *
 * @args
 * args[0] => File Path for the Train-Data in One-to-One mapping with Dimension form
 * args[1] => File Path for the input test data set to be aligned.
 *
 * @author Raktotpal Bordoloi
 *
 */
object AlignDataToDimension {
  val sc = new SparkConf().setAppName("Align-Dimensions").setMaster("local")
  val sparkContext = new SparkContext(sc)
  val sqlContext = new SQLContext(sparkContext)

  val delimiterChar = ","

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\winutils")

    val bagOfDimensions = loadPresetValues(args)

    val headerLine = extractHeaderFromDataSet(args)

    val headerSet = headerLine.split(delimiterChar)

    val appendingHeader: StringBuffer = new StringBuffer

    for (eachColumnName <- headerSet) {
      if (bagOfDimensions.contains(eachColumnName)) {
        println(bagOfDimensions.get(eachColumnName).get)
      } else {
        println("MISC")
      }
    }
  }

  def loadPresetValues(arg: Array[String]): HashMap[String, String] = {
    val valSeparator = ","
    val presetValues = sparkContext.textFile(arg(0))

    val presetBag = new HashMap[String, String]()

    for (eachEntry <- presetValues) {
      val eachLine = eachEntry.split(valSeparator)

      //            val isTrue = eachLine(1).replace("\"", "")
      //
      //            if (presetBag.contains(isTrue)) {
      //                println("##########################")
      //            }

      presetBag.+=((eachLine(1).replace("\"", ""), eachLine(0).replace("\"", "")))
    }

    return presetBag
  }

  def extractHeaderFromDataSet(args: Array[String]): String = {
    val testRDD = sparkContext.textFile(args(1))

    val headerLine = testRDD.take(1)(0)

    return headerLine
  }
}