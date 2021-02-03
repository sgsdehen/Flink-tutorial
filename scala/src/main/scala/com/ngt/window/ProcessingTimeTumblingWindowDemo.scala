package com.ngt.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author ngt
 * @create 2021-02-04 0:32
 */
object ProcessingTimeTumblingWindowDemo {
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

    keyed.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .reduce((value1, value2) => (value1._1, value1._2 + value2._2))
      .print()

    env.execute()
  }
}
