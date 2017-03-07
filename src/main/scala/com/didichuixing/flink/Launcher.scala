package com.didichuxing.flink

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by luojiangyu on 2/28/17.
  */
object Launcher {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    val stream = env.addSource(KafkaSourceConfig.getKafkaSource(args(0)))
    stream.flatMap(_.toLowerCase().split("\\W+"))
      .map(a => (a + "-" + new SimpleDateFormat("yyyy-MM-dd(HH:mm)").format(new Date(System.currentTimeMillis()))
        , 1))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
      .sum(1).print()

    env.execute()

  }

}
