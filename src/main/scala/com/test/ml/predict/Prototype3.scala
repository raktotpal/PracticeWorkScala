package com.test.ml.predict

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import scala.util.matching.Regex
import org.apache.spark.mllib.feature.Word2Vec

object Prototype3 {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\winutils")

    val sc = new SparkConf().setAppName("Test-N-Gram").setMaster("local")
    val sparkContext = new SparkContext(sc)
    val sqlContext = new SQLContext(sparkContext)

    val rdd = sparkContext.wholeTextFiles("C:\\Users\\Raktotpal\\Desktop\\trained")
    val newsgroups = rdd.map { case (file, text) => file.split("/").takeRight(2).head }
    val countByGroups = newsgroups.map(n => (n, 1)).reduceByKey(_ + _).collect.sortBy(-_._2).mkString("\n")
    // Tokenizing the text data
    val text = rdd.map { case (file, text) => text }
    val whiteSpaceSplit = text.flatMap(t => t.split(" ").map(_.toLowerCase))
    val nonWordSplit = text.flatMap(t => t.split("""\W+""").map(_.toLowerCase))
    val regex = """[^0-9]*""".r
    val filterNumbers = nonWordSplit.filter(token => regex.pattern.matcher(token).matches)
    val tokenCounts = filterNumbers.map(t => (t, 1)).reduceByKey(_ + _)
    val stopwords = Set("the", "a", "an", "of", "or", "in", "for", "by", "on", "but", "is", "not", "ith", "as", "was")
    val tokenCountsFilteredStopwords = tokenCounts.filter { case (k, v) => !stopwords.contains(k) }
    val tokenCountsFilteredSize = tokenCountsFilteredStopwords.filter { case (k, v) => k.size >= 2 }
    // filter out rare tokens with total occurence < 2
    val rareTokens = tokenCounts.filter { case (k, v) => v < 2 }.map { case (k, v) => k }.collect.toSet
    //        val tokenCountsFilteredAll = tokenCountsFilteredSize.filter { case (k, v) => !rareTokens.contains(k) }

    //        tokenCountsFilteredAll.collect.foreach(println)

    val tokens = text.map(doc => tokenize(doc, regex, stopwords, rareTokens))

    tokens.foreach(println)

    val word2vec = new Word2Vec().setVectorSize(3).setMinCount(0)
    word2vec.setSeed(42)
    // we do this to generate the same results each time
    val word2vecModel = word2vec.fit(tokens)
    word2vecModel.findSynonyms("research", 10).foreach(println)

  }

  // create a function to tokenize each document
  def tokenize(line: String, regEx: Regex, stopwords: Set[String], rareTokens: Set[String]): Seq[String] = {
    line.split("""\W+""")
      .map(_.toLowerCase)
      .filter(token => regEx.pattern.matcher(token).matches)
      .filterNot(token => stopwords.contains(token))
      .filterNot(token => rareTokens.contains(token))
      .filter(token => token.size >= 2).toSeq
  }

}