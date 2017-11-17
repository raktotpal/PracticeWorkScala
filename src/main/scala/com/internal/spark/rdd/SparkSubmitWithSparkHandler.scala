package com.internal.spark.rdd

import org.apache.log4j.Logger
import org.apache.spark.launcher.SparkLauncher

object SparkSubmitWithSparkHandler {
  object Holder extends Serializable {
    @transient lazy val log = Logger.getLogger(getClass.getName)
  }

  def main(args: Array[String]) {
    val javaHome = "/usr/java/jdk1.8.0_121"
    val sparkHome = "/opt/spark-2.1.0-bin-hadoop2.6"
    val appResource = "/root/PracticeWorkScala.jar"
    val mainClass = "com.internal.spark.rdd.DfTest"

    val sparkH = new SparkLauncher()
      .setVerbose(true)
      .setJavaHome(javaHome)
      .setSparkHome(sparkHome)
      .setAppResource(appResource)
      .setMainClass(mainClass)
      .setMaster("yarn")
      .setDeployMode("client")
      .setConf(SparkLauncher.DRIVER_MEMORY, "1g")
      .addAppArgs(args.mkString(","))
      .startApplication()

    val sparkAppId = sparkH.getAppId

    println("Spark App ID: " + sparkAppId)

  }

}

