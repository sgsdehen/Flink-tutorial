package com.ngt.window

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author ngt
 * @create 2021-02-03 21:55
 */
object EventTimeSessionWindowDemo {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger("rest.port", 8181)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val dataStream  = lines.map(data => {
      val strings: Array[String] = data.split(",")
      (strings(0).toLong, strings(1), strings(2).toInt)
    }).assignAscendingTimestamps(_._1)
      .setParallelism(1)
      .map(data => (data._2, data._3))

    dataStream.keyBy(_._1)
      .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
      .sum(1)
      .print()

    env.execute();
  }

}

/*
1609512630000,a,1
1609512631000,a,2
1609512634000,b,5
1609512635000,b,11
1609512636000,a,2
1609512641000,b,9

2> (1609512634000,b,25)
6> (1609512630000,a,3)
 */