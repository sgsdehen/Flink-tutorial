package com.ngt.wc

import org.apache.flink.streaming.api.scala._

/**
 * @author ngt
 * @create 2021-01-23 22:26
 * Scala WordCount
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val result: DataStream[(String, Int)] = lines
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)

    result.print()
    env.execute("StreamWordCount")
  }
}
