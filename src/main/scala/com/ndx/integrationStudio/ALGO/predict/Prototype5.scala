package com.ndx.integrationStudio.ALGO.predict

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import scala.collection.mutable.HashMap
import scala.util.control.Breaks._

object Prototype5 {
    val sc = new SparkConf().setAppName("Align-Dimensions").setMaster("local")
    val sparkContext = new SparkContext(sc)
    val sqlContext = new SQLContext(sparkContext)

    val delimiterChar = ","

    def main(args: Array[String]): Unit = {
        System.setProperty("hadoop.home.dir", "E:\\winutils")

        val bagOfDimensions = loadPresetValues()

        val testRDD = sparkContext.textFile("E:\\RPAL\\KproZ\\NIELSEN_INTEGRATION_STUDIO\\data\\CSVData.csv")
        val headerLine = testRDD.take(1)(0)

        val headerSet = headerLine.split(delimiterChar)

        val appendingHeader: StringBuffer = new StringBuffer

        for (eachColumnName <- headerSet) {
            if (bagOfDimensions.contains(eachColumnName)) {
                appendingHeader.append(eachColumnName)
            } else {
                var isFound = false
                breakable {
                    for (eachEntry <- bagOfDimensions) {
                        if (eachEntry._2.contains(eachColumnName)) {
                            appendingHeader.append(eachEntry._1).append(delimiterChar)
                            isFound = true
                            break
                        }
                    }
                }
                if (!isFound) {
                    appendingHeader.append(delimiterChar)
                }
            }
        }

        appendingHeader.deleteCharAt(appendingHeader.lastIndexOf(delimiterChar))

        testRDD.map(x =>
            if (x.equalsIgnoreCase(headerLine)) {
                appendingHeader.toString()
            } else { x }).saveAsTextFile("E:\\RPAL\\KproZ\\NIELSEN_INTEGRATION_STUDIO\\data\\CSVDataOUT")
    }

    def loadPresetValues(): HashMap[String, Seq[String]] = {
        val keyValSeparator = ":"
        val valSeparator = ","
        val presetValues = sparkContext.textFile("E:\\RPAL\\KproZ\\NIELSEN_INTEGRATION_STUDIO\\data\\BagOfHeaders.txt").toArray

        val presetBag = new HashMap[String, Seq[String]]()

        for (eachEntry <- presetValues) {
            val eachLine = eachEntry.split(keyValSeparator)

            presetBag.+=((eachLine(0), eachLine(1).split(valSeparator)))
        }

        return presetBag
    }

}