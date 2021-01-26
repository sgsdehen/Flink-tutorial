package com.ngt.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

/**
 * @author ngt
 * @create 2021-01-26 22:42
 */
object KafkaSinkDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val words: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    words.addSink(new FlinkKafkaProducer[String]("192.168.100.102:9092",
      "sinktest", new SimpleStringSchema()))

    env.execute()
  }
}
