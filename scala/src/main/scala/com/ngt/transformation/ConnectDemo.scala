package com.ngt.transformation

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

/**
 * @author ngt
 * @create 2021-02-02 1:54
 */
object ConnectDemo {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger("rest.port", 8181)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    // flink
    val lines1: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)
    // 15.0
    val lines2: DataStream[String] = env.socketTextStream("192.168.31.8", 9999)

    val map: DataStream[Double] = lines2.map(_.toDouble)

    // 允许两个流之间共享状态
    val connect: ConnectedStreams[String, Double] = lines1.connect(map)

    // 流1的类型，流2的类型，生成新流的类型
    connect.map(new CoMapFunction[String, Double, String] {
      override def map1(value: String): String = {
        value.toUpperCase();
      }

      override def map2(value: Double): String = {
        value * 10 + ""
      }
    }).print()
    env.execute()
  }
}
/*
  8888 输入
    flink
    spark
    flink
  9999 输入
    9.0
    19

 */