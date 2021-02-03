package com.ngt.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author ngt
 * @create 2021-02-04 0:32
 */
object ProcessingTimeTumblingWindowAllDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // saprk,1
    // scala,9
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)
    val nums: DataStream[Integer] = lines.map(Integer.parseInt(_).asInstanceOf[Integer])

    nums.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))).
      sum(0)
      .print()
    env.execute()
  }
}
