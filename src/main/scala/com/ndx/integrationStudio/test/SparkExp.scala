package com.internal.spark.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataType
//import org.apache.ignite.spark.IgniteContext
//import org.apache.ignite.configuration.IgniteConfiguration

object SparkExp {
    def main(args: Array[String]) {
        
      //val CONFIG = "/opt/rpal/apache-ignite-fabric-1.4.0-bin/config/default-config.xml"
        
        val sparkConf = new SparkConf().setAppName("IGNITE").setMaster("local")

        val sparkContext = new SparkContext(sparkConf);

//        val igniteContext = new IgniteContext[String, Int](sparkContext)
//        
//    	val cache = igniteContext.fromCache("testing1")
//    	
//    	cache.coalesce(1, false).saveAsTextFile("/home/raktotpal/Desktop/sparkTestOutIGNITED")
    	
    	//val result = cache.filter(_._2.contains("Ignite")).collect()
    	
    	sparkContext.stop
    }
    
    
//    def main(args: Array[String]) {
//    	System.setProperty("hadoop.home.dir", "E:\\winutils")
//
//        val sparkConf = new SparkConf().setAppName("SchemaInfer").setMaster("local")
//
//        val sparkContext = new SparkContext(sparkConf);
//
//        val file = sparkContext.textFile("C:\\Users\\Raktotpal\\Desktop\\sparkTest.txt")
//        
//        val schemaString = file.take(1).apply(0)
//        
//        var fields  = List[StructField]()
//        
//		for (fieldName <- schemaString.split(",")) {
//		    
//		    println("-----------------------==================== " + fieldName.getClass().getSimpleName())
//		    
//		  fields ::= DataTypes.createStructField(fieldName, DataTypes.StringType, true) 
//		}
//		
//        println("************************************** >>>> " + fields.length)
//
//    }
}