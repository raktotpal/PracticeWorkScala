package com.internal.spark.async

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit

object AsyncJob {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("TIMEOUT_TEST")
    val sc = new SparkContext(conf)
    val lst = List(1, 2, 3)
    // setting up an infite action
    val future = sc.parallelize(lst).map(while (true) _).collectAsync()

    try {
      Await.result(future, Duration(30, TimeUnit.SECONDS))
      println("success!")
    } catch {
      case _: Throwable =>
        future.cancel()
        println("timeout")
    }

    // sleep for 1 hour to allow inspecting the application in yarn
    Thread.sleep(60 * 60 * 1000)
    sc.stop()
  }
}
