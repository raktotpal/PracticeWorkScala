package com.internal.scala.partition

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructField

object ParquetUtilityV2 {
    def usage() {
        println("Usage::")
        println("""* args(0) - input
			     * args(1) - output
			     * args(2) - input format -> csv / parquet
			     * args(3) - output format -> csv / parquet
                 * args(4) - compressCodec to write data
			     * args(5) - isHeader : Boolean
			     * args(6) - delimiter
			     * args(7) - inferSchema : Boolean
			     * args(8) - customschema : String""")
    }

    /**
     * args(0) - input
     * args(1) - output
     * args(2) - input format -> csv / parquet
     * args(3) - output format -> csv / parquet
     * args(4) - compressCodec to write data
     * args(5) - isHeader : Boolean
     * args(6) - delimiter
     * args(7) - inferSchema : Boolean
     * args(8) - customschema : String
     *
     */
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("PARQUET-UTILITY").setMaster("local")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        var parsedDF: DataFrame = null

        val inputPath = args(0).trim()
        val outputPath = args(1).trim()
        val inputFormat = args(2).trim()
        val outputFormat = args(3).trim()
        val compressionCodec = args(4).trim()
        val isHeaderInInput = args(5).trim()
        val inputDelimiter = args(6).trim()
        val inferSchemaInInput = args(7).trim()
        var customSchema: String = StringUtils.EMPTY

        try {
            customSchema = args(8).trim()
        } catch {
            case e: Exception => println("custom schema not provided")
        }

        if (inputFormat.equalsIgnoreCase("csv"))
            parsedDF = parseCSV(sqlContext, inputPath, isHeaderInInput.toBoolean, inputDelimiter, inferSchemaInInput.toBoolean, customSchema)
        else if (inputFormat.equalsIgnoreCase("parquet"))
            parsedDF = readParquet(sqlContext, inputPath)

        if (outputFormat.equalsIgnoreCase("csv"))
            pushAsCSV(parsedDF, true, compressionCodec, outputPath)
        else if (outputFormat.equalsIgnoreCase("parquet"))
            pushAsParquet(parsedDF, outputPath)

    }

    /**
     * Takes the input path of CSV file as parameter and create dataframe instance out of it.
     * Uses DataBrick's spark-csv utility.
     *
     * @param context SQLContext
     * @param inputPath Input path for CSV file.
     * @param isHeader TRUE if first line of CSV needs to be treated as HEADER.
     * @param delimiter Delimiter of CSV file.
     * @param inferSchema TRUE if we need to infer schema automatically.
     */
    def parseCSV(sqlContext: SQLContext, inputPath: String, isHeader: Boolean, delimiter: String, inferSchema: Boolean, customSchema: String): DataFrame = {
        var df: DataFrame = null

        if (customSchema.isEmpty()) {
            df = sqlContext.read
                .format("com.databricks.spark.csv")
                .option("header", isHeader.toString) // Use first line of all files as header
                .option("inferSchema", inferSchema.toString) // Automatically infer data types
                .load(inputPath)
        } else {
            val schemaStrucType = StructType(customSchema.split(",")
                .map(fieldName => StructField(fieldName.split(":")(0), fieldName.split(":")(1) match { case "int" => IntegerType case "integer" => IntegerType case "string" => StringType }, true)))

            df = sqlContext.read
                .format("com.databricks.spark.csv")
                .option("header", isHeader.toString) // Use first line of all files as header
                .schema(schemaStrucType)
                .load(inputPath)
        }

        println("Printing Custom Schema.....")
        df.printSchema
        df.show

        return df
    }

    /**
     * Read a parquet file and construct a DataFrame.
     *
     *
     * @param inputPath Input file path (absolute path) for the parquet file.
     * @return DataFrame object
     */
    def readParquet(sqlContext: SQLContext, inputPath: String): DataFrame = {
        val parquetDf = sqlContext.parquetFile(inputPath)
        return parquetDf
    }

    /**
     * Read a CSV file; Infer first line as HEADER and constructs schema; and convert it to PARQUET format.
     *
     * @param inputPath Input Path for the CSV file.
     * @param outputPath Output path for Parquet file.
     */
    def convertToParquet(sqlCtx: SQLContext, inputPath: String, outputPath: String): Unit = {
        val fileRDD = sqlCtx.sparkContext.textFile(inputPath)

        val schemaString = fileRDD.take(1).apply(0)
        var fields = List[StructField]()

        var i: Int = 0
        for (fieldName <- schemaString.split(",")) {
            fields ::= DataTypes.createStructField("col-" + i, StringType, true)
            i = i + 1
        }

        val schema = StructType(fields)
        val myRowRDD = fileRDD.map(p => p.split(",", -1)).map(x => Row.fromSeq(x))

        val mydf = sqlCtx.createDataFrame(myRowRDD, schema)
        mydf.saveAsParquetFile(outputPath)
    }

    /**
     * Create Parquet file from a dataFrame instance.
     *
     * @param parsedDF Dataframe object holding data to be convefrted to PARQUET format.
     * @param isHeader TRUE if HEADER needs to be written in output Parquet file.
     * @param compressCodec CompressCodec to be used to write parquet file.
     * @param outputPath Output path for the Parquet file.
     */
    def pushAsCSV(parsedDF: DataFrame, isHeader: Boolean, compressCodec: String, outputPath: String) {
        if (compressCodec.equalsIgnoreCase("false")) {
            parsedDF.write
                .format("com.databricks.spark.csv")
                .option("header", isHeader.toString)
                .option("codec", compressCodec)
                .save(outputPath)
        } else {
            parsedDF.write
                .format("com.databricks.spark.csv")
                .option("header", isHeader.toString)
                .option("codec", compressCodec)
                .save(outputPath)
        }
    }

    /**
     * Create Parquet file from the Dataframe instance.
     *
     * @param parsedDF Dataframe to be converted to Parquet format.
     * @param outputPath Otput path for the parquet file to be written.
     */
    def pushAsParquet(parsedDF: DataFrame, outputPath: String) {
        parsedDF.saveAsParquetFile(outputPath)
    }
}