package com.ngt.function

import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author ngt
 * @create 2021-02-05 15:50
 */
object ProcessingTimeTimerDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val wordAndOne: DataStream[(String, Int)] = lines.process(new ProcessFunction[String, (String, Int)]() {
      override def processElement(value: String,
                                  ctx: ProcessFunction[String, (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
        val words: Array[String] = value.split(" ")

        for (elem <- words) {
          out.collect((elem, 1))
        }
      }
    })
    wordAndOne.keyBy(_._1)
      .process(new KeyedProcessFunction[String, (String, Int), (String, Int)]() {

        override def processElement(value: (String, Int), ctx: KeyedProcessFunction[String, (String, Int), (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
          // 注册一个10s定时器
          val currentProcessingTime: Long = ctx.timerService().currentProcessingTime()
          println("定时器注册时间：" + currentProcessingTime + " 定时器触发时间：" + (currentProcessingTime + 10000))
          ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 30 * 1000L)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Int), (String, Int)]#OnTimerContext, out: Collector[(String, Int)]): Unit = {
          println("定时器触发了: " + timestamp)
        }
      })
      .print()

    env.execute()
  }
}
