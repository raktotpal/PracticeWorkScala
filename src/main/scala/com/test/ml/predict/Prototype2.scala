package com.test.ml.predict

import java.io.File
import org.apache.spark.ml.tuning.CrossValidator

object Prototype2 {
  def main(args: Array[String]) {
    val basePath = "C:\\Users\\Raktotpal\\Desktop\\KNN_Example_1.csv"
    val testData = getDataFromCSV(new File(basePath))

  }

  def getDataFromCSV(file: File): (Array[Array[Double]], Array[Int]) = {
    val source = scala.io.Source.fromFile(file)
    val data = source
      .getLines()
      .drop(1)
      .map(x => getDataFromString(x))
      .toArray

    source.close()

    val dataPoints = data.map(x => x._1)
    val classifierArray = data.map(x => x._2)
    return (dataPoints, classifierArray)
  }

  def getDataFromString(dataString: String): (Array[Double], Int) = {

    //Split the comma separated value string into an array of strings
    val dataArray: Array[String] = dataString.split(',')

    //Extract the values from the strings
    val xCoordinate: Double = dataArray(0).toDouble
    val yCoordinate: Double = dataArray(1).toDouble
    val classifier: Int = dataArray(2).toInt

    //And return the result in a format that can later 
    //easily be used to feed to Smile
    return (Array(xCoordinate, yCoordinate), classifier)
  }
}