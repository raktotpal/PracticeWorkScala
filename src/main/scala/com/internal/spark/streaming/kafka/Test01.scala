package com.internal.spark.streaming.kafka

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import kafka.serializer.DefaultDecoder

object Test01 {
  def main(args: Array[String]) {
    // brokerList: localhost:9092; zkQuorum: localhost:2181
    val Array(zkQuorum, brokerList, group, topics, numPartitionsOfInputTopic, numThreads) = args
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaWordCount")

    // Make sure you give enough cores to your Spark Streaming application.  You need cores for running "receivers"
    // and for powering the actual the processing.  In Spark Streaming, each receiver is responsible for 1 input
    // DStream, and each receiver occupies 1 core.  If all your cores are occupied by receivers then no data will be
    // processed!
    //val inputTopic = KafkaTopic(topics)
    //val cores = inputTopic.partitions + 1

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Set up the input DStream to read from Kafka (in parallel)
    val kafkaStream = {
      //val sparkStreamingConsumerGroup = "spark-streaming-consumer-group"
      val kafkaParams = Map(
        "zookeeper.connect" -> zkQuorum,
        "group.id" -> group,
        "zookeeper.connection.timeout.ms" -> "1000")

      // The code below demonstrates how to read from all the topic's partitions.  We create an input DStream for each
      // partition of the topic, unify those streams, and then repartition the unified stream.  This last step allows
      // us to decouple the desired "downstream" parallelism (data processing) from the "upstream" parallelism
      // (number of partitions).
      //
      // Note: In our case the input topic has only 1 partition, so you won't see a real effect of this fancy setup.
      //
      // And yes, the way we do this looks quite strange -- we combine a hardcoded `1` in the topic map with a
      // subsequent `(1..N)` construct.  But this approach is the recommended way.
      val streams = (1 to Integer.parseInt(numPartitionsOfInputTopic)) map { _ =>
        KafkaUtils.createStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
          ssc,
          kafkaParams,
          Map(topics -> 1),
          storageLevel = StorageLevel.MEMORY_ONLY_SER // or: StorageLevel.MEMORY_AND_DISK_SER
          ).map(_._2)
      }

      streams.foreach(print)

      val unifiedStream = ssc.union(streams) // Merge the "per-partition" DStreams
      val sparkProcessingParallelism = 1 // You'd probably pick a higher value than 1 in production.

      // Repartition distributes the received batches of data across specified number of machines in the cluster
      // before further processing.  Essentially, what we are doing here is to decouple processing parallelism from
      // reading parallelism (limited by #partitions).
      unifiedStream.repartition(sparkProcessingParallelism)
    }

    // We use accumulators to track the number of consumed and produced messages across all tasks.
    val numInputMessages = ssc.sparkContext.accumulator(0L, "Kafka messages consumed")
    val numOutputMessages = ssc.sparkContext.accumulator(0L, "Kafka messages produced")

    // We use a broadcast variable to share a pool of Kafka producers, which we use to write data from Spark to Kafka.
    val producerPool = {
      val pool = KafkaProducerPool.createKafkaProducerPool(brokerList, topics)
      ssc.sparkContext.broadcast(pool)
    }

    kafkaStream.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val p = producerPool.value.borrowObject()
        partitionOfRecords.foreach {
          case tweet =>
            //val bytes = converter.value.apply(tweet)
            p.send(tweet, tweet, Some(topics + "out"))
            numOutputMessages += 1
        }
        producerPool.value.returnObject(p)
      })
    })
  }
}