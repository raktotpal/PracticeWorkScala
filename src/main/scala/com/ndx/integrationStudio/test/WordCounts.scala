package com.internal.spark.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//import org.apache.ignite.configuration.IgniteConfiguration
//import org.apache.ignite.Ignite
//import org.apache.ignite.spark.IgniteContext
//import org.apache.ignite.spark.IgniteContext

object WordCounts {
    def main(args: Array[String]) {
        System.setProperty("hadoop.home.dir", "E:\\winutils")

        val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local")
        
        
     //   val CONFIG = "/opt/rpal/apache-ignite-fabric-1.4.0-bin/config/default-config.xml"
            
        val sparkContext = new SparkContext(sparkConf);

//        val igniteContext = new IgniteContext[String, Int](sparkContext)

        val file = sparkContext.textFile("C:\\Users\\Raktotpal\\Desktop\\sparkTest.txt")
//        	// ("/home/raktotpal/Desktop/sparkTest.txt")

        val words = file.flatMap(line => tokenize(line))

        val wordCounts1 = words.map(x => (x, 1)).reduceByKey(_ + _)
//
//        println("--------------------")
//        wordCounts1.saveAsTextFile("C:\\Users\\Raktotpal\\Desktop\\sparkTestOut")
//        
//        
//        val cacheRdd = igniteContext.fromCache("testing1")
//
//        cacheRdd.savePairs(wordCounts1, true)
        
        sparkContext.stop
    }

    private def tokenize(text: String): Array[String] = {
        // Lowercase each word and remove punctuation.
        text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s,]", "").split("[\\s+,]")
    }
}