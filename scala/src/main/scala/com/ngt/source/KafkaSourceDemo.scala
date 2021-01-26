package com.ngt.source

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

/**
 * @author ngt
 * @create 2021-01-26 19:15
 */
object KafkaSourceDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    //指定组ID
    properties.setProperty("group.id", "gwc10")
    //如果没有记录偏移量，第一次从最开始消费
    properties.setProperty("auto.offset.reset", "earliest")
    //kafka的消费者不自动提交偏移量
    //properties.setProperty("enable.auto.commit", "false")

    val lines: DataStream[String] =
      env.addSource(new FlinkKafkaConsumer[String]("wc10", new SimpleStringSchema(), properties))

    lines.print()
    env.execute()
  }
}
