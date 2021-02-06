package com.ngt.chain

import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author ngt
 * @create 2021-02-06 22:13
 */
object OperatorChainDemo {
  def main(args: Array[String]): Unit = {
    val configuration: Configuration = new Configuration()
    configuration.setInteger("rest.port", 8181)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    // 系统默认开启 OperatorChaining ，可以手动关闭
    env.disableOperatorChaining
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
    })

    filterd.map((_, 1))
      .keyBy(_._1)
      .sum(1)
      .print()
    env.execute()
  }
}

/*

    Source: Socket Stream     (Parallelism：1)  --rebalance-->
    Flat Map -> Filter -> Map (Parallelism：8)  --hash-->
    Keyed Aggregation -> Sink: Print to Std. Out(Parallelism：8)

    算子链，默认情况下处于开启状态
    Flink 将算子的 subtasks 链接成 tasks。每个 task 由一个线程执行。将算子链接成 task 是个有用的优化：
    它减少线程间切换、缓冲的开销，并且减少延迟的同时增加整体吞吐量。

    当出现下列情形的时候会切换算子链：
    1. 并行度发生变化
    2. 发生物理分区的分区的时候，如：shuffle，Rebalancing，Rescaling 等
 */