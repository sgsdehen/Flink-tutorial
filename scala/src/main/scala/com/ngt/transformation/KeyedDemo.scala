package com.ngt.transformation

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala._

/**
 * @author ngt
 * @create 2021-02-02 0:20
 */
object KeyedDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    // 1. 使用下标，只适用于元组，新版中已过时
    lines.map((_, 1)).keyBy(0).sum(1).print()

    // 2. 使用 KeySelector
    lines.map((_, 1)).keyBy(new KeySelector[(String, Int), String] {
      override def getKey(value: (String, Int)): String = {
        value._1
      }
    }).sum(1).print()

    // 3. 使用 lambda 表达式
    lines.map((_, 1)).keyBy(_._1).sum(1).print()
    env.execute()
  }
}
