package com.ngt.window


import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
/**
 * @author ngt
 * @create 2021-02-04 0:55
 */
object TumblingWindowJoinDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines1: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    env.setParallelism(1)

    val leftStream: DataStream[(Long, String, Int)] = lines1.map(data => {
      val strings: Array[String] = data.split(",")
      (strings(0).toLong, strings(1), strings(2).toInt)
    }).assignAscendingTimestamps(_._1)

    val lines2: DataStream[String] = env.socketTextStream("192.168.31.8", 9999)

    val rightStream: DataStream[(Long, String, Int)] = lines2.map(data => {
      val strings: Array[String] = data.split(",")
      (strings(0).toLong, strings(1), strings(2).toInt)
    }).assignAscendingTimestamps(_._1)

    leftStream.join(rightStream)
      .where(_._2)
      .equalTo(_._2)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply((left,right) =>(left._1,right._1,left._2,left._3,right._3))
      .print()

    env.execute()
  }
}
