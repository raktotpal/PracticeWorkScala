package com.internal.spark.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataType

object RC {
    val sparkConf = new SparkConf().setAppName("IGNITE").setMaster("local")
    val sparkContext = new SparkContext(sparkConf);

    
    
    def main(args: Array[String]) {

        println("=======================>>")

        val x = "masterFilePath=/home/export.csv,inputAppData=1|2|3;4|5|6,alignedData=aa|bb"
        
        val instructions = x.split(",")

        var paramList = new Array[String](3)

        for (elem <- instructions) {
            
            println("********** " + elem)
            
            val key = String.valueOf(elem.substring(0, elem.indexOf('='))).trim()
            val value = String.valueOf(elem.substring(elem.indexOf('=') + 1, elem.length()))
            
            println("00000000 KEY 00000000" + key)

            key match {
                case "masterFilePath" => paramList(0) = value
                case "inputAppData" => paramList(1) = value
                case "alignedData" => paramList(2) = value
                case _ => println("NO MATCH")
            }

        }
        
        paramList.toList.foreach(println)
        
//        val hdfsPath = sparkContext.textFile("/user/bardra01/text.txt");
//    	
//    	hdfsPath.foreach(row => println("=====**** " + row))
//    	
//    	println("=======================>>")
//    	
//    	
//    	val local = sparkContext.textFile("file:///home/bardra01/myTest/text.txt");
//    	
//    	local.foreach(println)
//    	
//    	
//    	println("=======================>>")
    	
    	
    }
    
    
    /*
     * Convert immutable Map to Mutable Map
     */
    def main2(args: Array[String]) {
        var realMap = scala.collection.mutable.Map[String, String]()

        val testData = sparkContext.textFile("E:\\RPAL\\KproZ\\NIELSEN_INTEGRATION_STUDIO\\data\\CSVData.csv")
        val mapVal = testData.map(x => (x.charAt(0).toString, x)).collect().toMap

        realMap = collection.mutable.Map(mapVal.toSeq: _*)

        val x = sparkContext.makeRDD(realMap.toSeq)

        x.map(x => Array(x._1, "," + x._2).mkString).foreach(println)
    }

    /*
     * Get Key from a Map base on Value-part.
     */
    def main1(args: Array[String]) {
        var testMap = Map[String, Int]()

        testMap += "AA" -> 4

        testMap += "A5" -> 5

        val it = testMap.toIterator

        import scala.util.control.Breaks._

        breakable {
            while (it.hasNext) {
                val x = it.next
                if (4 == x._2) {
                    println(x._1)
                    break
                }
            }
        }
    }
}