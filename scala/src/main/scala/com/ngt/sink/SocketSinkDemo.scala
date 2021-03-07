package com.ngt.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._

/**
 * @author ngt
 * @create 2021-01-26 20:10
 */
object SocketSinkDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    lines.print()
    lines.writeToSocket("192.168.31.8", 9999, new SimpleStringSchema())
    env.execute()
  }
}
