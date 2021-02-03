package com.ngt.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

/**
 * @author ngt
 * @create 2021-02-03 20:28
 * 不使用sum聚合，使用Reduce进行聚合
 */
object CountWindowAllReduceDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val nums: DataStream[Int] = lines.map(_.toInt)
    val window: AllWindowedStream[Int, GlobalWindow] = nums.countWindowAll(4, 2)
    window.reduce(new ReduceFunction[Int] {
      override def reduce(value1: Int, value2: Int): Int = {
        value1 + value2
      }
    }).print()
    env.execute()
  }
}
