package com.ngt.function

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author ngt
 * @create 2021-02-05 17:48
 *         使用测流输出数据中的偶数
 *         https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/stream/side_output.html
 */
object SideOutputDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val oddOutputTag: OutputTag[String] = new OutputTag[String]("odd") {}
    val evenOutputTag: OutputTag[String] = new OutputTag[String]("even") {}
    val nanOutputTag: OutputTag[String] = new OutputTag[String]("nan") {}

    val mainStream: DataStream[String] = lines.process(new ProcessFunction[String, String] {
      override def processElement(value: String,
                                  ctx: ProcessFunction[String, String]#Context,
                                  out: Collector[String]): Unit = {
        try {
          val i: Int = value.toInt
          if ((i & 1) == 1) {
            ctx.output(oddOutputTag, value)
          } else {
            ctx.output(evenOutputTag, value)
          }
        } catch {
          case ex: Exception => {
            ctx.output(nanOutputTag, value)
          }
        }

        // 主流中输出全部的数据，否则主流没有输出
        out.collect(value)
      }
    })

    mainStream.getSideOutput(evenOutputTag).print("even")
    mainStream.getSideOutput(oddOutputTag).print("odd")
    mainStream.print("main")
    env.execute()
  }
}
