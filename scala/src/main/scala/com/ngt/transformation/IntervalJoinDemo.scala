package com.ngt.transformation

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @author ngt
 * @create 2021-02-03 0:37
 */
object IntervalJoinDemo {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger("rest.port", 8181)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    // DataStream1
    val lines1: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)


    val wordAndOne1: DataStream[(String, Int)] = lines1.map(data => {
      val strings: Array[String] = data.split(",")
      (strings(0).toLong, strings(1), strings(2).toInt)
    })
      .assignAscendingTimestamps(data => data._1)
      .map(data => (data._2, data._3))


    // DataStream2
    val lines2: DataStream[String] = env.socketTextStream("192.168.31.8", 9999)

    val wordAndOne2: DataStream[(String, Int)] = lines2.map(data => {
      val strings: Array[String] = data.split(",")
      (strings(0).toLong, strings(1), strings(2).toInt)
    })
      .assignAscendingTimestamps(data => data._1)
      .map(data => (data._2, data._3))


    wordAndOne1
      .keyBy(_._1)
      .intervalJoin(wordAndOne2.keyBy(_._1))
      .between(Time.seconds(-2), Time.seconds(2))
      .process((left: (String, Int), right: (String, Int), ctx: ProcessJoinFunction[(String, Int), (String, Int), (String, Int)]#Context, out: Collector[(String, Int)]) => {
        out.collect(left._1, left._2 + right._2)
      })
      .print()
    env.execute()
  }
}

/*
1609512632000,a,2
1609512630000,a,9
(a,11)
1609512632000,b,1
1609512633000,b,3
(b,4)
1609512641000,d,1
1609512644000,d,6
没有输出

 */