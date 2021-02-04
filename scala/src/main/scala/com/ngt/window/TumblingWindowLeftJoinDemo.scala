package com.ngt.window

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.{lang, util}

/**
 * @author ngt
 * @create 2021-02-04 0:55
 */
object TumblingWindowLeftJoinDemo {
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


    leftStream.coGroup(rightStream)
      .where(_._2)
      .equalTo(_._2)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply(new CoGroupFunction[(Long,String,Int),(Long,String,Int),(Long,String,Int,Long,Int)]{
        override def coGroup(first: lang.Iterable[(Long, String, Int)],
                             second: lang.Iterable[(Long, String, Int)],
                             out: Collector[(Long, String, Int, Long, Int)]): Unit = {
          val firstValue: util.Iterator[(Long, String, Int)] = first.iterator()
          val secondValue: util.Iterator[(Long, String, Int)] = second.iterator()
        }
      })
  }
}
