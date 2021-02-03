package com.ngt.window

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author ngt
 * @create 2021-02-03 21:19
 */
object EvenTimeTumblingWindowDemo {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger("rest.port", 8181)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    // 1609512630000,a,1
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val dataStream  = lines.map(data => {
      val strings: Array[String] = data.split(",")
      (strings(0).toLong, strings(1), strings(2).toInt)
    }).assignAscendingTimestamps(_._1)
      .setParallelism(1)
      .map(data => (data._2, data._3))


    // 如果 dataStream 并行度大于1 那么每个窗口的时间满足条件才会触发窗口
    dataStream.keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .sum(1)
      .print()

    env.execute()
  }
}
