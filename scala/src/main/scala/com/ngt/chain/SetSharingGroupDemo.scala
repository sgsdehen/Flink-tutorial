package com.ngt.chain

import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author ngt
 * @create 2021-02-06 22:15
 */
object SetSharingGroupDemo {
  def main(args: Array[String]): Unit = {
    val configuration: Configuration = new Configuration()
    configuration.setInteger("rest.port", 8181)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val words: DataStream[String] = lines.flatMap(new FlatMapFunction[String, String] {
      override def flatMap(value: String, out: Collector[String]): Unit = {
        val words: Array[String] = value.split(" ")
        for (elem <- words) {
          out.collect(elem)
        }
      }
    })

    val filterd: DataStream[String] = words.filter(new FilterFunction[String] {
      override def filter(value: String): Boolean = {
        value.startsWith("error")
      }
    }).setParallelism(4)
      .disableChaining()
      .slotSharingGroup("doit") // 后面共享资源槽的名字都是 doit

    filterd.map((_, 1))
      .slotSharingGroup("default") // 切换到默认的资源槽
      .keyBy(_._1)
      .sum(1)
      .print()

    env.execute()
  }
}
