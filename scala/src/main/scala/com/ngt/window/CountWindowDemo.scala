package com.ngt.window

import org.apache.flink.streaming.api.scala._

/**
 * @author ngt
 * @create 2021-02-03 20:32
 */
object CountWindowDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // saprk,1
    // scala,9
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)
    val wordAndCount: DataStream[(String, Int)] = lines.map(data => {
      val strings: Array[String] = data.split(",")
      (strings(0), strings(1).toInt)
    })

    val keyed: KeyedStream[(String, Int), String] = wordAndCount.keyBy(_._1)
    // 组内增量聚合，组内达到指定的条数就进行触发输出
    keyed.countWindow(3).sum(1).print()
    //    keyed.countWindow(10,5).sum(1).print()
    env.execute()
  }

}
