package com.ngt.transformation

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._

/**
 * @author ngt
 * @create 2021-02-02 2:42
 */
object ReduceDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val words: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val wordAndOne: DataStream[(String, Int)] = words.map((_, 1))

    val keyed: KeyedStream[(String, Int), String] = wordAndOne.keyBy(_._1)

    // 使用 reduce 实现 wordcount
    val reduced: DataStream[(String, Int)] = keyed.reduce(new ReduceFunction[(String, Int)] {
      override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
        (value1._1, value1._2 + value2._2)
      }
    })

    reduced.print()
    env.execute("ReduceDemo")
  }
}
