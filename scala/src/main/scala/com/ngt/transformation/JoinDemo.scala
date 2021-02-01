package com.ngt.transformation

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author ngt
 * @create 2021-02-02 2:47
 */
object JoinDemo {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger("rest.port", 8181)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    // DataStream1
    val lines1: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)
    val wordAndOne1: DataStream[(String, Int)] = lines1
      .flatMap(_.split(" "))
      .map((_, 1))

    // DataStream2
    val lines2: DataStream[String] = env.socketTextStream("192.168.31.8", 9999)
    val wordAndOne2: DataStream[(String, Int)] = lines2
      .flatMap(_.split(" "))
      .map((_, 1))


    val keyBy1: KeyedStream[(String, Int), String] = wordAndOne1.keyBy(_._1)
    val keyBy2: KeyedStream[(String, Int), String] = wordAndOne1.keyBy(_._1)

    keyBy1.join(keyBy2)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .apply((first, second) => (first._1, first._2 + second._2))
      .print()

    env.execute();

  }
}
