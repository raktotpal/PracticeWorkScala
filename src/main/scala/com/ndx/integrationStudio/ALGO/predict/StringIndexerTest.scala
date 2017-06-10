package com.ndx.integrationStudio.ALGO.predict

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object StringIndexerTest {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "E:\\winutils")

    val sc = new SparkConf().setAppName("Test-N-Gram").setMaster("local")
    val sparkContext = new SparkContext(sc)
    val sqlContext = new SQLContext(sparkContext)

    val df = sqlContext.createDataFrame(
      Seq((0, "a b"), (1, "c b"), (2, "b a"), (3, "a b"), (4, "a c"), (5, "a b"), (6, "b c"))).toDF("id", "category")
    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
    val indexed = indexer.fit(df).transform(df)
    indexed.show()
  }
}