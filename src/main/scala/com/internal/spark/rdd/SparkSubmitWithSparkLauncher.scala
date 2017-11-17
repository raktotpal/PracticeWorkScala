package com.internal.spark.rdd

import org.apache.log4j.Logger
import org.apache.spark.launcher.SparkLauncher

object SparkSubmitWithSparkLauncher {
  object Holder extends Serializable {
    @transient lazy val log = Logger.getLogger(getClass.getName)
  }

  def main(args: Array[String]) {
    val javaHome = "/usr/java/jdk1.8.0_121"
    val sparkHome = "/opt/spark-2.1.0-bin-hadoop2.6"
    val yarnConf = "/etc/hadoop/conf"
    val appResource = "/root/PracticeWorkScala.jar" // "/my/app.jar"
    val mainClass = "com.internal.spark.rdd.DfTest" // "my.spark.app.Main"

    val sparkL = new SparkLauncher
    sparkL.setVerbose(true)
      .setJavaHome(javaHome)
      .setSparkHome(sparkHome)
      .setConf("spark.yarn.config.gatewayPath", yarnConf)
      //.setConf("YARN_CONF_DIR", yarnConf)
      .setAppResource(appResource)
      .setMainClass(mainClass)
      .setMaster("yarn")
      .setConf(SparkLauncher.DRIVER_MEMORY, "1g")
      .addAppArgs(args.mkString(","))

    // Launches a sub-process that will start the configured Spark application.
    val proc = sparkL.launch();

    val inputStreamReaderRunnable = new InputStreamReaderRunnable(proc.getInputStream(), "input")
    val inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
    inputThread.start();

    val errorStreamReaderRunnable = new InputStreamReaderRunnable(proc.getErrorStream(), "error")
    val errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error")
    errorThread.start();

    println("Waiting for finish...")
    val exitCode = proc.waitFor()
    println("Finished! Exit code:" + exitCode)

  }

}

