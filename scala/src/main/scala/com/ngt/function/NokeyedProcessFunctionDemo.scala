package com.ngt.function

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author ngt
 * @create 2021-02-05 15:11
 */
object NokeyedProcessFunctionDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    lines.process(new ProcessFunction[String, (String, Int)]() {
      override def processElement(value: String,
                                  ctx: ProcessFunction[String, (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
        val words: Array[String] = value.split(" ")

        for (elem <- words) {
          out.collect((elem, 1))
        }
      }
    })
      .print()

    env.execute()
  }
}
