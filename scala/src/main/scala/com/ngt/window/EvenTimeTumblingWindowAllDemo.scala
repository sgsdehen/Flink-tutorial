package com.ngt.window

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

/**
 * @author ngt
 * @create 2021-02-03 20:45
 */
object EvenTimeTumblingWindowAllDemo {
  def main(args: Array[String]): Unit = {

    val conf: Configuration = new Configuration()
    conf.setInteger("rest.port", 8181)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    // DataStream1
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val wordAndCount: DataStream[(Long, Int)] = lines.map(data => {
      val strings: Array[String] = data.split(",")
      (strings(0).toLong, strings(1).toInt)
    })//.assignAscendingTimestamps(_._1)  // 设置EvenTime
      //.setParallelism(1)

    // 设置EvenTime
    val value: DataStream[(Long, Int)] = wordAndCount.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ZERO)
      .withTimestampAssigner(new SerializableTimestampAssigner[(Long, Int)] {
        override def extractTimestamp(element: (Long, Int), recordTimestamp: Long): Long = element._1
      }))
      .setParallelism(1)

    value.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
      .sum(1)
      .print()
    env.execute()
  }
}

/*
输入
1609512630000,1
1609512631000,2
1609512634000,5
1609512634998,3
1609512634999,7
1609512635000,11 触发
1609512639999,16
1609512640000,18 触发

输出
(1609512630000,18) 1+2+5+3+7
(1609512635000,27) 11+16
闭区间 一个窗口中包含， [0000,4999]， 但是需要 5000 才能触发
 */
