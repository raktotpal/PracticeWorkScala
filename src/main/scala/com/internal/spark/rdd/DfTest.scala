package com.internal.spark.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.Row
import scala.collection.mutable.HashMap
import java.util.Arrays
import scala.util.Random
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hive.ql.parse.JoinType
import org.apache.log4j.Logger
import java.util.ArrayList

object DfTest {
  object Holder extends Serializable {
    @transient lazy val log = Logger.getLogger(getClass.getName)
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SchemaInfer").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val metaTableSchema = "TableName,ColumnName,Type,PK,Parent,DimensionName"

    val rowVals = List("pdmTableName", "sDescCol", "dimension", "c", "ccc", "market")
    val schema_rdd = getSchemaRdd(metaTableSchema)
    val rowInstance = sc.parallelize(Seq(rowVals))
    val rowRdd = rowInstance.map(x => Row.fromSeq(rowVals))

    println("---------------------------- 3 ")

    val finalData = sqlContext.createDataFrame(rowRdd, schema_rdd)

    finalData.printSchema
    //println("---------------------------- 4 " + finalData.count)

    finalData.foreach(x => println("------------------------------------>>> " + x))
  }

  def main1(args: Array[String]) {
    val x = new ArrayList[String]
    x.add("pal")
    x.add("bordoloi")

    println("result:: " + x.contains("pal1"))
  }

  def main2(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SchemaInfer").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //        val fileRDD = sc.textFile("/user/bardra01/rpal/test/testParse.csv")
    val fileRDD = sc.textFile("D:\\DATA\\testParse.csv")
    //        try {
    //        println(fileRDD.collect()(0).trim())
    //        } catch {
    //          case e1 : Exception => {
    //            println("------------->>")
    //          }
    //        }

    val schemaString = fileRDD.take(1).apply(0)

    println("schemaString === " + schemaString)

    var fields = List[StructField]()

    for (fieldName <- schemaString.split(",")) {
      fields :+= DataTypes.createStructField(fieldName, DataTypes.StringType, true)
    }
    //fields = fields.reverse

    val rowRDDval = fileRDD.filter(x => !x.equalsIgnoreCase(schemaString)).map(x => Row.fromSeq(x.split(",")))

    var sampleDF = sqlContext.createDataFrame(rowRDDval, StructType(fields))

    sampleDF.printSchema

    // sampleDF.rdd.saveAsTextFile("D:\\DATA\\TEST\\myInput")
    println("sampleDF.count:: " + sampleDF.count)

    val x = getHierInfo(sampleDF)

    val itr = x.keysIterator

    while (itr.hasNext) {
      val xy = itr.next;

      println(xy + " :: ------------------------------->>>>>>")

      for (each <- x.get(xy)) {

        val itr1 = each.keysIterator
        while (itr1.hasNext) {
          val xy1 = itr1.next;

          println(xy1 + " :: ---->>>>>>")

          for (each1 <- each.get(xy1).get) {
            println("last level:: " + each1)
          }

        }

      }
    }

    sampleDF.registerTempTable("masterTableName")

    val finalDf = injectHierIndexInDataframe(sqlContext, "masterTableName", x)

    finalDf.printSchema

    println("finalDf.count:: " + finalDf.count)

    finalDf.rdd.saveAsTextFile("D:\\DATA\\TEST\\myDF")

    //        
    //        // RDDUtil.fixSchemaInDataframe(sampleDF).show
    //        
    //        // sampleDF.rdd.foreach(println)
    //        
    //        CsvParquetUtilV2.pushAsCSV(sampleDF, true, "false", "C:\\Users\\RAKTOTPAL\\Desktop\\NDX_DATA\\csv_op_101")

    //        CsvParquetUtilV2.pushAsCSV(sampleDF, true, "false", "C:\\Users\\RAKTOTPAL\\Desktop\\NDX_DATA\\csv_op_1")

    //        val schemaInp = sampleDF.schema
    //        var fieldsCaps = List[StructField]()
    //        for (eachField <- schemaInp) {
    //            var refinedColumnName = eachField.name.replaceAll("[\\W&&[^\\\\/]]", "_").toUpperCase()
    //            fieldsCaps :+= StructField(refinedColumnName, eachField.dataType, eachField.nullable)
    //        }
    //        val x = sampleDF.sqlContext.createDataFrame(sampleDF.rdd, StructType(fieldsCaps))

    //sampleDF.show

    //        val x = sampleDF.columns
    //        
    //        for (eachColumnName <- x) {
    //                //val refinedColumnName = eachColumnName.replaceAll("[ ,;{}()\\n\\r\\t=]", "_")
    //                var refinedColumnName = eachColumnName.replaceAll("[\\W&&[^\\\\/]]", "_").toUpperCase()
    //                
    //                if (!sampleDF.equals(eachColumnName)) {
    //                    println("---------------------- " + eachColumnName + " => " + refinedColumnName)
    //                    sampleDF = sampleDF.withColumnRenamed("`"+eachColumnName+"`", refinedColumnName)
    //                }
    //                println("column names parsed:: " + refinedColumnName)
    //                //sampleDF.printSchema
    //            }
    //        
    //        //sampleDF.printSchema
    //        
    //        sampleDF.show

    //sampleDF.saveAsParquetFile("C:\\Users\\Raktotpal\\Desktop\\NDX_DATA\\testParseCsv_2.parquet")

    //        CsvParquetUtilV2.pushAsCSV(sampleDF, true, "false", "E:\\Movies\\R\\abc.csv");

    //        var x = List[String]()
    //        sampleDF.dtypes.foreach(each => x ::= each._1)
    //        //x => if (x._2.equalsIgnoreCase("StringType1")) println(x._1)
    //
    //        x.foreach(println)
    //
    //        println("_________________________")
    //
    //        val schemaMod = sampleDF.schema
    //        var fieldsMod = List[StructField]()
    //
    //        var fieldIndex = 0
    //        for (eachField <- schemaMod) {
    //            if (fieldsMod.contains(DataTypes.createStructField(eachField.name, eachField.dataType, eachField.nullable))) {
    //                fieldsMod ::= DataTypes.createStructField(eachField.name + "_" + fieldIndex, eachField.dataType, eachField.nullable)
    //            } else {
    //                fieldsMod ::= DataTypes.createStructField(eachField.name, eachField.dataType, eachField.nullable)
    //            }
    //            fieldIndex = fieldIndex + 1
    //        }
    //        fieldsMod = fieldsMod.reverse
    //
    //        println(fieldsMod.size)

    //        val modifiedDF = sqlContext.createDataFrame(sampleDF.rdd, StructType(fieldsMod))
    //
    //        modifiedDF.printSchema
    //        
    //        modifiedDF.select(modifiedDF.col("name_3")).show
  }

  def getHierInfo(inputDF: DataFrame): HashMap[String, HashMap[String, Array[String]]] = {
    val schemaInp = inputDF.schema
    val hierInfo = new HashMap[String, HashMap[String, Array[String]]]
    for (eachField <- schemaInp) {
      if (eachField.name.endsWith("_HIER_NAME")) {
        val dimName = eachField.name.split("_")(0)

        val hier = inputDF.select(dimName + "_HIER_NAME", dimName + "_HIER_LVL", dimName + "_LEVEL_CODE")

        hier.registerTempTable("test1")

        val hierInfo2 = new HashMap[String, Array[String]]

        for (eachHier <- hier.select(dimName + "_HIER_NAME").collect) {

          println("checking eachHier.getString(0) :: " + eachHier.getString(0))

          val distinctHierDf = inputDF.sqlContext.sql("select " + dimName + "_HIER_LVL, " + dimName + "_LEVEL_CODE from test1 where " + dimName + "_HIER_NAME = '" + eachHier.getString(0) + "'")
          //orderby " + dimName + "_LEVEL_CODE
          //hier.where(dimName + "_HIER_NAME = " + eachHier.getString(0)).select(dimName + "_HIER_LVL", dimName + "_LEVEL_CODE").distinct.orderBy(dimName + "_LEVEL_CODE")
          //.where(hier.col(dimName + "_HIER_NAME") == eachHier)

          //hierInfo2.put(eachHier.getString(0), distinctHierDf.select(dimName + "_HIER_LVL", dimName + "_LEVEL_CODE").orderBy(dimName + "_LEVEL_CODE").map(row => row(0).asInstanceOf[String]).collect())
          hierInfo2.put(eachHier.getString(0),
            distinctHierDf.select(dimName + "_HIER_LVL", dimName + "_LEVEL_CODE")
              .orderBy(dimName + "_LEVEL_CODE").rdd
              .map(x => x.get(0).toString()).collect())
        }
        hierInfo.put(eachField.name, hierInfo2)
      }
    }
    hierInfo
  }

  def injectHierIndexInDataframe(hiveContext: SQLContext, masterTableName: String, hierInfo: HashMap[String, HashMap[String, Array[String]]]): DataFrame = {
    Holder.log.info("Injesting Hierarchy index in to main dataframe")

    hiveContext.udf.register("hashfunc", ((s: String) => if (s != null) DigestUtils.sha256Hex(s) else ""))

    val masterDf = hiveContext.table(masterTableName)

    val hierList = new HashMap[String, Int]
    val hierQueryList = new ArrayList[DataFrame]
    for (eachHier <- hierInfo.values) {
      for (eachHierChar <- eachHier.values) {
        for (eachHier <- eachHierChar) {
          hierList.put(eachHier, 0)
        }
      }
    }

    var mCols = ""
    for (eachColInMaster <- masterDf.columns) {
      mCols += eachColInMaster + ","
    }
    var selectHierHashQuery = "SELECT " + mCols
    for (eachHier <- hierList.keys) {
      selectHierHashQuery += "hashfunc(" + eachHier + ") AS " + eachHier + "_idx,"
      //val hierDf = hiveContext.sql("SELECT hashfunc(" + eachHier + ") AS " + eachHier + "_idx, " + eachHier + " FROM " + masterTableName)
    }
    if (selectHierHashQuery.endsWith(",")) {
      selectHierHashQuery = selectHierHashQuery.dropRight(1)
    }
    selectHierHashQuery += " FROM " + masterTableName

    val finalDf = hiveContext.sql(selectHierHashQuery)
    //masterDf.join(hierDf.select(eachHier + "_idx").distinct, masterDf.col(eachHier) === hierDf.col(eachHier), "inner")
    finalDf
  }

  def getSchemaRdd(metaTableSchema: String): StructType = {
    StructType(metaTableSchema.split(",").map(fieldName => StructField(fieldName.trim(), StringType, true)))
  }
}

