package com.ngt.function

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author ngt
 * @create 2021-02-05 18:25
 */
object WindowLateDateDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val timeAndCount: DataStream[(String, Int)] = lines
      .assignAscendingTimestamps(_.split(",")(0).toLong)
      .map(data => {
        val split: Array[String] = data.split(",")
        (split(1), split(2).toInt)
      })

    // 处理迟到数据
    val lastoutputTag: OutputTag[(String, Int)] = new OutputTag[(String, Int)]("last") {}

    val sumed: DataStream[(String, Int)] = timeAndCount.keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .sideOutputLateData(lastoutputTag)
      .sum(1)

    sumed.print()
    sumed.getSideOutput(lastoutputTag).print("lastdata")

    env.execute()
  }
}


/*
1609512630000,a,1
1609512631000,b,3
1609512632000,c,5
1609512633000,b,6
1609512635000,a,2
1609512636000,a,1
1609512638000,c,8
1609512639000,b,9
1609512640000,a,3

 */