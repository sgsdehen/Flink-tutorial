package com.ngt.demo.networkflow

import com.ngt.demo.userbehavior.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author ngt
 * @create 2021-05-21 22:00
 * 计算独立访客的数量
 */
object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream: DataStream[String] = env.readTextFile("data/UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = inputStream.map(data => {
      val arr: Array[String] = data.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    }
    )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val uvStream: DataStream[UvCount] = dataStream
      .filter(_.behavior == "pv")
      .windowAll(TumblingEventTimeWindows.of(Time.minutes(60L)))
      .apply(new UvCountResult())

    uvStream.print()
    env.execute()
  }
}

// 定义输出Uv统计样例类
case class UvCount(windowEnd: Long, count: Long)

class UvCountResult() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    var userIdSet: Set[Long] = Set[Long]()
    for (userBehavior <- input) userIdSet += userBehavior.userId
    out.collect(UvCount(window.getEnd, userIdSet.size))
  }
}