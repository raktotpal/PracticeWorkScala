package com.ndx.integrationStudio

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CollectionsUtils
import java.util.Locale
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ StringType, StructField, StructType }
import org.apache.spark.sql.types._

object AlignDimension {

  //private val dictionary = new FrequencyMap(data)
  private val alphabet = 'a' to 'z'
  var dictionary = Map.empty[String, Int]

  /**Map that will return a default value of zero for no entry*/
  /*private class FrequencyMap[K](m: Map[K, Int]) extends scala.collection.mutable.HashMap[K, Int] {
    override def default(key: K) = 0
    this ++ m
  }*/

  private def edit1(word: String): Seq[String] = {
    // "hello" becomes ("", "hello"), ("h", "ello"), etc. 
    val splits = (0 to word.length).map(i => (word.take(i), word.drop(i)))
    val deleted = splits.filter(_._2.length > 0)
      .map(tuple => tuple._1 + tuple._2.drop(1))
    val transposed = splits.filter(_._2.length > 1)
      .map(tuple => tuple._1 + tuple._2(1) + tuple._2(0) + tuple._2.drop(2))
    val replaced = splits.filter(_._2.length > 0)
      .flatMap(tuple => alphabet.map(tuple._1 + _ + tuple._2.drop(1)))
    val inserted = splits
      .flatMap(tuple => alphabet.map(tuple._1 + _ + tuple._2))
    deleted ++ transposed ++ replaced ++ inserted
  }

  private def edit2(word: String, edits: Seq[String]) = {
    val edit2 = for (
      edit <- edits;
      e2 <- edit1(edit);
      known = (e2, dictionary.get(e2).getOrElse(0));
      if (known._2 > 0)
    ) yield known
    edit2.foldLeft((word, 0)) { best(_, _) }
  }

  private def best(a: (String, Int), b: (String, Int)) = if (b._2 > a._2) b else a

  private def fix(word: String, edits: Seq[String]) = {
    val terms = edits.map(term => (term, dictionary.get(term).getOrElse(0)))
    val correction = terms.foldLeft((word, 0)) { best(_, _) }
    if (correction._2 > 0) correction._1 else edit2(word, edits)._1
  }

  def correct(word: String) =
    if (dictionary.contains(word)) word else fix(word, edit1(word))

  def tokenize(line: String): Seq[String] = {
    val regex = """[^0-9]*""".r

    val stopwords = Set(
      "the", "a", "an", "of", "or", "in", "for", "by", "on", "but", "is", "not",
      "with", "as", "was", "if",
      "they", "are", "this", "and", "it", "have", "from", "at", "my",
      "be", "that", "to")

    val specialChars = Set(",", ".", "\\", "\"", "-", "#", "$", "%", "&", "*", "(", ")", "[", "]", "{", "}")

    line.split(", ").map(_.toLowerCase).map { word =>
      var wrd = ""
      for (sp <- specialChars) {
        if (word.contains(sp)) {
          wrd = word.replace(sp, "")
        }
      }
      if (wrd.isEmpty()) word else wrd
    }
      .filter(token => regex.pattern.matcher(token).matches)
      .filterNot(token => stopwords.contains(token))
      .filter(token => token.size >= 2)
      .toSeq

  }

  def load(bagOfWordsFile: RDD[String]) = {

    val words = bagOfWordsFile.flatMap(line => tokenize(line))

    val wordCount = words.map(word => (word, 1)).reduceByKey(_ + _)

    dictionary = dictionary ++ wordCount.map(x => (x._1, x._2)).collect.toMap

  }

  /**
   * The resulting similarity ranges from −1 meaning exactly opposite, to 1
   * meaning exactly the same, with 0 usually indicating independence, and
   * in-between values indicating intermediate similarity or dissimilarity.
   *
   * For text matching, the attribute vectors A and B are usually the term
   * frequency vectors of the documents. The cosine similarity can be seen as
   * a method of normalizing document length during comparison.
   *
   * In the case of information retrieval, the cosine similarity of two
   * documents will range from 0 to 1, since the term frequencies (tf-idf
   * weights) cannot be negative. The angle between two term frequency vectors
   * cannot be greater than 90°.
   *
   * @param leftVector
   * @param rightVector
   * @return
   */
  def consineTextSimilarity(left1: String, right1: String): Double = {
    val left = left1.toLowerCase().split("[_/ \\.]")
    val right = right1.toLowerCase().split("[_/ \\.]")
    // left: Array[String], right: Array[String]

    val leftWordCountMap = new HashMap[String, Integer]();
    val rightWordCountMap = new HashMap[String, Integer]();
    val uniqueSet = new HashSet[String]();
    var temp: Integer = null;
    for (leftWord <- left) {
      temp = leftWordCountMap.getOrElse(leftWord, 0)
      if (temp == 0) {
        leftWordCountMap.put(leftWord, 1);
        uniqueSet.add(leftWord);
      } else {
        leftWordCountMap.put(leftWord, temp + 1);
      }
    }
    for (rightWord <- right) {
      temp = rightWordCountMap.getOrElse(rightWord, 0)
      if (temp == 0) {
        rightWordCountMap.put(rightWord, 1);
        uniqueSet.add(rightWord);
      } else {
        rightWordCountMap.put(rightWord, temp + 1);
      }
    }
    val leftVector = new Array[Integer](uniqueSet.size) //int[uniqueSet.size];

    val rightVector = new Array[Integer](uniqueSet.size) //int[uniqueSet.size()];
    var index = 0
    var tempCount: Integer = null
    for (uniqueWord <- uniqueSet) {
      tempCount = leftWordCountMap.getOrElse(uniqueWord, 0)
      leftVector(index) = tempCount // == null ? 0: tempCount
      tempCount = rightWordCountMap.getOrElse(uniqueWord, 0)
      rightVector(index) = tempCount // == null ? 0: tempCount
      index = index + 1
    }
    return consineTextSimilarity(leftVector, rightVector);
  }

  def consineTextSimilarity(leftVector: Array[Integer], rightVector: Array[Integer]): Double = {
    if (leftVector.length != rightVector.length)
      return 1;
    var dotProduct = 0.0;
    var leftNorm = 0.0;
    var rightNorm = 0.0;
    for (i <- 0 until leftVector.length) {
      dotProduct = dotProduct + leftVector(i) * rightVector(i)
      leftNorm = leftNorm + leftVector(i) * leftVector(i)
      rightNorm = rightNorm + rightVector(i) * rightVector(i)
    }

    val result = dotProduct / (Math.sqrt(leftNorm) * Math.sqrt(rightNorm));
    return result;
  }

  def getRecommendation(bagOfWords: RDD[(String, String)], correctedMap: Map[String, Iterable[(String, String)]]): Array[String] = {
    var recommendArray = Array[String]()

    for (correction <- correctedMap.keys) {
      val scoresRDD = bagOfWords.map(x => (x._2, consineTextSimilarity(x._2, correction)))

      //      println("Correction : " + correction)

      val maxKey = scoresRDD.takeOrdered(1)(Ordering[Double].reverse.on(_._2))(0) //._1
      var classlabel = bagOfWords.map(x => (x._2, x._1)).lookup(maxKey._1).head

      //      println("Classlabel : " + classlabel)

      classlabel = if (classlabel.equalsIgnoreCase("DIM_NAME")) "MISC" else classlabel

      recommendArray = new Array[String]((correctedMap.get(correction).size))

      for (lines <- (correctedMap.get(correction).get)) {
        //        println(lines)
        recommendArray :+ (lines._2 + "," + classlabel + "," + (maxKey._2 * 100) + "\n")
      }
    }

    recommendArray
  }

  val conf = new SparkConf().setAppName("DimensionRecommendation").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\winutils")

    //    val bagOfWordsFilePath = args(0)
    //    val headerFilePath = args(1)
    //
    //    val bagOfWordsFile = sc.textFile(bagOfWordsFilePath)
    //    val headerFile = sc.textFile(headerFilePath)
    //
    //    load(bagOfWordsFile)
    //
    //    val corrected = headerFile.map { line =>
    //      val splits = line.split(",")
    //      val normalised = splits(5).toLowerCase(Locale.ENGLISH)
    //      val correction = correct(normalised);
    //
    //      (line, correction)
    //    }
    //
    //    val bagOfWords = bagOfWordsFile.map { line =>
    //      val splits = line.split(",")
    //      (splits(0), splits(1))
    //    }
    //
    //    val correctedMap = corrected.map(row => (row._2, row._1)).groupBy(row => row._1).collect().toMap
    //
    //    val recommendArray = getRecommendation(bagOfWords, correctedMap)
    //    
    //    recommendArray.foreach(println)

    val recommendArray = Array[String]("21,91,db1,179,PRODUCT,PRODUCT,92,db2,182,PRODUCT,PRODUCT,80", "21,93,db2,183,PRODUCT,PRODUCT,92,db2,182,PRODUCT,PRODUCT,90", "21,94,db2,186,PRODUCT,PRODUCT,92,db2,182,PRODUCT,PRODUCT,65", "21,95,db2,191,PRODUCT,PRODUCT,92,db2,182,PRODUCT,PRODUCT,80", "21,91,db1,177,FACT,FACT,92,db2,181,FACT,FACT,100", "21,93,db2,185,FACT,FACT,92,db2,181,FACT,FACT,68", "21,94,db2,187,FACT,FACT,92,db2,181,FACT,FACT,75", "21,95,db2,190,FACT,FACT,92,db2,181,FACT,FACT,100")

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
      .map(p => Row(p(0).trim, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim, p(6).trim, p(7).trim, p(8).trim, p(9).trim))
      .toSeq

    // Creating RDD[Row]
    val rowRDD = sc.makeRDD(rowArray)

    val dataFrame = sqlContext.createDataFrame(rowRDD, schema)

    dataFrame.registerTempTable("totalDF")

    val res = sqlContext.sql("DESCRIBE totalDF");

    val query = "SELECT AlignedDimID FROM totalDF"

    val res1 = sqlContext.sql(query)

    res1.foreach(row => println(row.getString(0)))

    //        res1.foreach(row => 
    //
    //        executeSQL(row.getString(0))
    //        
    //        )

    //        val res2 = sqlContext.sql(query)
    //        
    //        res1.foreach(println)

  }

  def executeSQL(dimensionID: String): Unit = {
    val mySQL = "SELECT * FROM totalDF WHERE AlignedDimID = \"" + dimensionID + "\""

    val df1 = sqlContext.sql(mySQL)

    df1.foreach(x => println(x))
  }
}