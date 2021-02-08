package com.ngt.state

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala._
/**
 * @author ngt
 * @create 2021-02-08 22:46
 */
object MapStateDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)))
    env.enableCheckpointing(5000)

    val tpDataStream: DataStream[(String, String, Double)] = lines.map(data => {
      val words: Array[String] = data.split(",")
      (words(0), words(1), words(2).toDouble)
    })

    tpDataStream.keyBy(_._2)
      .process()

  }
}
