package com.test.ml
import org.apache.spark.{ SparkConf, SparkContext }
import java.io._
import com.rockymadden.stringmetric.similarity._

object AlignLabelToDimension {
  val conf = new SparkConf().setAppName("Spark AssociationRule").setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    val data = sc.textFile("E:\\RPAL\\KproZ\\NIELSEN_INTEGRATION_STUDIO\\data\\export.csv")
    val dataRDD = data.map(s => s.split(","))
    val dataRDDLookup = dataRDD.map(s => (s(0), s(1)))

    val header = sc.textFile("E:\\RPAL\\KproZ\\NIELSEN_INTEGRATION_STUDIO\\data\\GUM360NGA\\GUM360NGA.csv").take(1)(0).split(",")

    def distanceScore(string1: String, string2: String): Double = {
      var jarowinkle: Double = 0
      var jaccard: Double = 0
      var levenshtein: Double = 0
      val jw = JaroWinklerMetric.compare(string1, string2)

      if (jw.isDefined) {
        jarowinkle = jw.get
      }

      val jm = JaccardMetric(1).compare(string1, string2)

      if (jm.isDefined) {
        jaccard = jw.get
      }

      val lm = LevenshteinMetric.compare(string1, string2)

      if (lm.isDefined) {
        levenshtein = lm.get
      }

      val score = jarowinkle + jaccard + levenshtein
      return score
    }

    val pw = new PrintWriter(new File("E:\\RPAL\\KproZ\\NIELSEN_INTEGRATION_STUDIO\\data\\GUM360NGA\\class_predictions.txt"))
    for (colname <- header) {
      val scoresRDD = dataRDDLookup.map(x => (x._2, distanceScore(x._2, colname)))
      val maxKey = scoresRDD.takeOrdered(1)(Ordering[Double].reverse.on(_._2))(0)._1
      val classlabel = dataRDDLookup.map(x => (x._2, x._1)).lookup(maxKey).head
      pw.write(colname + "," + classlabel + "\n")
    }
    pw.close()

  }
}