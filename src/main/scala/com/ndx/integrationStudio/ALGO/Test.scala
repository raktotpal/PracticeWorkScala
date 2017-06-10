package com.internal.spark.ALGO

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.HiveContext

object Test {
  val sc = new SparkConf().setAppName("Align-Dimensions").setMaster("local")
  val sparkContext = new SparkContext(sc)
  val sqlContext = new SQLContext(sparkContext)

  val hc = new HiveContext(sparkContext)

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "E:\\winutils")

    val testRDD = sparkContext.textFile("E:\\RPAL\\KproZ\\NIELSEN_INTEGRATION_STUDIO\\data\\dataTest.txt")

    val testRDD1 = sparkContext.textFile("E:\\RPAL\\KproZ\\NIELSEN_INTEGRATION_STUDIO\\data\\dataTest1.txt")

    //        testRDD.subtract(testRDD1).saveAsTextFile("E:\\RPAL\\KproZ\\NIELSEN_INTEGRATION_STUDIO\\data\\OKOUT")

    val headerLine = testRDD.take(1)(0)
    val headerSet = headerLine.split(",")

    val rowRDDval = testRDD.map(x => Row.fromSeq(Seq(x)))

    val e = rowRDDval.map { x => x.mkString }
    //.filter(_.split(",")(0).contains(""))
    //        val structVal : StructType = (StructField("", StringType, true), StructField("", StringType, true))

    //        val rddDataframe = sqlContext.createDataFrame(rowRDDval, structVal)

    val schema = StructType(List(StructField("class", StringType, true), StructField("content", StringType, true)))

    val df = sqlContext.createDataFrame(rowRDDval, schema)

    df.registerTempTable("OK")

    val x = sqlContext.sql("SELECT * FROM OK")

    e.saveAsTextFile("E:\\RPAL\\KproZ\\NIELSEN_INTEGRATION_STUDIO\\data\\OKOUT")
    //.save("E:\\RPAL\\KproZ\\NIELSEN_INTEGRATION_STUDIO\\data\\OKOUT")

    //        
    //        val alignedDF = sqlContext.sql("SELECT " + selectClause.substring(0, selectClause.length()-2) + " FROM " + regTmpTbl)
    //val datasetOutput = this.namedRdds.get[Row]("RDD_DATASET_OUTPUT")
    //if (!datasetOutput.isDefined)
    //{ 
    //    this.namedRdds.getOrElseCreate("RDD_DATASET_OUTPUT", alignedDF.rdd, true) 
    //    //alignedDF.rdd.saveAsTextFile("/tmp/myoutput/jars_"+inputDatasetId) 
    //    } else { logger.info("Check for the presence of dataset Id : " + inputDatasetId.toString());
    //    val existing_union_rdd = this.namedRdds.get[Row]("RDD_DATASET_OUTPUT"); 
    //    //val chk_exist_rdd = existing_union_rdd.map{x:Row => x.filter(_.split(",")(o).contains(inputDatasetId)) 
    //    val chk_exist_rdd = existing_union_rdd.map{x:Row => x.getAs[String](o)}.filter(_.split(",")(o).contains(inputDatasetId)) 
    //    logger.info("Filter applied to find dataset.."); 
    //    logger.info("Filtered count is: " + chk_exist_rdd.count()); 
    //    logger.info("unioning to existing RDD_DATASET_OUTPUT")
    //    val unionedRDD = this.namedRdds.get[Row]("RDD_DATASET_OUTPUT").get.union(alignedDF.rdd) 
    //    logger.info("Unioned RDD size:" + unionedRDD.count().toString()) 
    //    this.namedRdds.update("RDD_DATASET_OUTPUT", unionedRDD) 
    //    //unionedRDD.saveAsTextFile("/tmp/myoutput/jars_"+inputDatasetId) 
    //    }
  }

}