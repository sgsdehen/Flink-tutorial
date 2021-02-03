package com.ngt.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author ngt
 * @create 2021-02-04 0:14
 */
object ProcessingTimeSlidingWindowAllDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val nums: DataStream[Integer] = lines.map(Integer.parseInt(_).asInstanceOf[Integer])

    nums.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      .sum(0).print()
    env.execute()
  }

}
