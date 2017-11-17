package com.internal.spark.rdd

import java.util.Arrays
import java.util.Properties

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkSubmit

import com.google.common.collect.Lists
import scala.collection.mutable.ArrayBuffer
//import org.apache.spark.deploy.ClientArguments
import org.apache.spark.deploy.yarn.ClientArguments

object SparkSubmitWithSparkClient {
  def main(args: Array[String]): Unit = {
    val javaHome = "/usr/java/jdk1.8.0_121"
    val sparkHome = "/opt/spark-2.1.0-bin-hadoop2.6"
    val yarnConf = "/etc/hadoop/conf"
    val appResource = "/root/PracticeWorkScala.jar" // "/my/app.jar"
    val mainClass = "com.internal.spark.rdd.DfTest" // "my.spark.app.Main"

    val cl = new SparkSubmitWithSparkClient("spark-job-from-client", mainClass, appResource, new Properties, null, null)
    cl.submit()
  }
}

/**
 * Submits a job to YARN in cluster mode
 */
class SparkSubmitWithSparkClient(jobName1: String, jobClass1: String, applicationJar1: String, sparkProperties1: Properties,
    additionalJars1: Array[String], files1: Array[String]) {

  var jobName = jobName1
  var jobClass = jobClass1
  var applicationJar = applicationJar1
  var additionalJars: Array[String] = additionalJars1
  var files: Array[String] = files1
  var sparkProperties: Properties = sparkProperties1

  def submit() {

    val args = ArrayBuffer("--verbose", "--name", jobName, "--jar", applicationJar, "--class", jobClass);

    if (additionalJars != null && additionalJars.length > 0) {
      args.+=("--addJars");
      args.+=(StringUtils.join(additionalJars, ","));
    }

    if (files != null && files.length > 0) {
      args.+=("--files");
      args.+=(StringUtils.join(files, ","));
    }

    if (sparkProperties.getProperty("spark.executor.cores") != null) {
      args.+=("--executor-cores");
      args.+=(sparkProperties.getProperty("spark.executor.cores"));
    }

    if (sparkProperties.getProperty("spark.executor.memory") != null) {
      args.+=("--executor-memory");
      args.+=(sparkProperties.getProperty("spark.executor.memory"));
    }

    if (sparkProperties.getProperty("spark.driver.cores") != null) {
      args.+=("--driver-cores");
      args.+=(sparkProperties.getProperty("spark.driver.cores"));
    }

    if (sparkProperties.getProperty("spark.driver.memory") != null) {
      args.+=("--driver-memory");
      args.+=(sparkProperties.getProperty("spark.driver.memory"));
    }

    // identify that you will be using Spark as YARN mode
    System.setProperty("SPARK_YARN_MODE", "true");

    val sparkConf = new SparkConf();
    sparkConf.set("spark.yarn.preserve.staging.files", "true");

    val it = sparkProperties.entrySet().iterator()

    while (it.hasNext()) {
      val e = it.next();
      sparkConf.set(e.getKey().toString(), e.getValue().toString());
    }

    println("Spark args: ", args.toList)
    println("Spark conf settings: " + sparkConf.getAll.toList);

    SparkSubmit.main(args.toArray)

    //val cArgs = new ClientArguments(args)
    //val client = new Client(cArgs, sparkConf, sparkConf)
    //val client = new Client(cArgs, new Configuration(), sparkConf);
    //client.run();
  }

}