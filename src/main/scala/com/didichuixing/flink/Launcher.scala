package com.didichuxing.flink

import java.util.{Date, Properties}

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema


/**
  * Created by luojiangyu on 2/28/17.
  */
object Launcher {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    /* Checkpoint */
    env.enableCheckpointing(60000L)

    val consumerProperties = new Properties
    consumerProperties.put("bootstrap.servers", args(0))
    consumerProperties.put("group.id", args(1))

    val stream = env.addSource(new FlinkKafkaConsumer010[String](args(2), new SimpleStringSchema, consumerProperties))
    stream.flatMap(_.toLowerCase().split("\\W+"))
      .map(a => (a,1))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
      .sum(1)
      .print()

    env.execute()

  }

}
