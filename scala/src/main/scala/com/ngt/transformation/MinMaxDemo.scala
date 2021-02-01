package com.ngt.transformation

import org.apache.flink.streaming.api.scala._

/**
 * @author ngt
 * @create 2021-02-02 1:15
 */
object MinMaxDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val keyed: KeyedStream[(String, Int), String] = lines.map(line => (line.split(",")(0), line.split(",")(1).toInt))
      .keyBy(_._1)

    keyed.min(1).print()
    keyed.max(1).print()
    env.execute()

  }

}
