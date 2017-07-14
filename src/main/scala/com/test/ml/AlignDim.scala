package com.test.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.json.JSONObject
import org.json.JSONArray

object AlignDim {
  val conf = new SparkConf().setAppName("DimensionRecommendation").setMaster("local")
  val sparkContext = new SparkContext(conf)
  val sqlContext = new SQLContext(sparkContext)

  def main(args: Array[String]) {

    val recommendArray = Array[String]("21,91,db1,179,PRODUCT,PRODUCT,92,db2,182,PRODUCT,PRODUCT,80",
      "21,93,db2,183,PRODUCT,PRODUCT,92,db2,182,PRODUCT,PRODUCT,90",
      "21,94,db2,186,PRODUCT,PRODUCT,92,db2,182,PRODUCT,PRODUCT,65",
      "21,95,db2,191,PRODUCT,PRODUCT,92,db2,182,PRODUCT,PRODUCT,80",
      "21,91,db1,177,FACT,FACT,92,db2,181,FACT,FACT,100",
      "21,93,db2,185,FACT,FACT,92,db2,181,FACT,FACT,68",
      "21,94,db2,187,FACT,FACT,92,db2,181,FACT,FACT,75",
      "21,95,db2,190,FACT,FACT,92,db2,181,FACT,FACT,100")

    // Generate the schema based on the string of schema
    val schema = StructType(List(StructField("project_id", StringType),
      StructField("dataset_id", StringType),
      StructField("dataset_name", StringType),
      StructField("dimension_id", StringType),
      StructField("dimension_name", StringType),
      StructField("prediction", StringType),
      StructField("AlignedDataSetID", StringType),
      StructField("AlignedDataSetName", StringType),
      StructField("AlignedDimID", StringType),
      StructField("AlignedDimName", StringType),
      StructField("prediction_", StringType),
      StructField("ConfidenceLevel", StringType)))

    // Convert records of the RDD (people) to Rows
    val rowArray = recommendArray.map(_.split(","))
      .map(p => Row(p(0).trim, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim,
        p(6).trim, p(7).trim, p(8).trim, p(9).trim))
      .toSeq

    // Creating RDD[Row]
    val rowRDD = sparkContext.makeRDD(rowArray)

    val dataFrame = sqlContext.createDataFrame(rowRDD, schema)

    dataFrame.registerTempTable("totalDF")

    val res = sqlContext.sql("DESCRIBE totalDF");

    val query = "SELECT AlignedDimID FROM totalDF"

    val res1 = sqlContext.sql(query)

    val dimsID = res1.distinct.collect
    //.map(row => row.getString(0)).distinct.collect

    val dimMappingModel = new JSONObject

    val aligned = new JSONArray

    val lh = new JSONArray

    for (eachEntry <- dimsID) {

      val currentResult = sqlContext.sql("SELECT * FROM totalDF WHERE AlignedDimID = '" + eachEntry + "'")
      currentResult.registerTempTable("currentTableRecords")

      val lhPart = new JSONObject

      //            currentResult.foreach(row =>  )

    }

  }

}