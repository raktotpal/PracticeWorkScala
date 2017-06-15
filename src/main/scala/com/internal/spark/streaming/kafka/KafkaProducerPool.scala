package com.internal.spark.streaming.kafka

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.commons.pool2.impl.GenericObjectPool

object KafkaProducerPool {
  def createKafkaProducerPool(brokerList: String, topic: String): GenericObjectPool[KafkaProducerApp] = {
    val producerFactory = new BaseKafkaProducerAppFactory(brokerList, defaultTopic = Option(topic))
    val pooledProducerFactory = new PooledKafkaProducerAppFactory(producerFactory)
    val poolConfig = {
      val c = new GenericObjectPoolConfig
      val maxNumProducers = 10
      c.setMaxTotal(maxNumProducers)
      c.setMaxIdle(maxNumProducers)
      c
    }
    new GenericObjectPool[KafkaProducerApp](pooledProducerFactory, poolConfig)
  }
}