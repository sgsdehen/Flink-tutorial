package com.ngt.window

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author ngt
 * @create 2021-02-03 22:58
 */

object ProcessingTimeSessionWindowDemo {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger("rest.port", 8181)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    // DataStream1
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val wordAndCount: DataStream[(String, Int)] = lines.map(data => {
      val strings: Array[String] = data.split(",")
      (strings(0), strings(1).toInt)
    })

    val keyed: KeyedStream[(String, Int), String] = wordAndCount.keyBy(_._1)

    keyed.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
      .sum(1)
      .print()

    //    keyed.window(ProcessingTimeSessionWindows.withDynamicGap(element => element._2 * 5000))
    //      .sum(1)
    //      .print()
    env.execute()
  }
}
