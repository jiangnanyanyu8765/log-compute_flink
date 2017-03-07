package com.didichuxing.flink

import java.io.File
import java.util.Properties

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
  * Created by luojiangyu on 2/28/17.
  */
class KafkaSourceConfig {
  private val confLocation = "conf" + File.separator + "consumer.properties"

  def initProperties() = {
    val properties = new Properties
    val inputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream(confLocation)
    properties.load(inputStream)
    properties
  }

}

object KafkaSourceConfig {
  val kafkaSourceConfig = new KafkaSourceConfig

  def getKafkaSource(topic: String): FlinkKafkaConsumer09[String] = {
    val properties = kafkaSourceConfig.initProperties()
    new FlinkKafkaConsumer09[String](topic, new SimpleStringSchema, properties)
  }

}
