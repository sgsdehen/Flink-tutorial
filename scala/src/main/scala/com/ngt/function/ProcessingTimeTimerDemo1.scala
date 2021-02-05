package com.ngt.function

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author ngt
 * @create 2021-02-05 16:25
 */
object ProcessingTimeTimerDemo1 {
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
        lazy private val counter: ValueState[Integer] =
          getRuntimeContext.getState(new ValueStateDescriptor[Integer]("kvstate", classOf[Integer]))

        override def processElement(value: (String, Int),
                                    ctx: KeyedProcessFunction[String, (String, Int), (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
          val currentProcessingTime: Long = ctx.timerService().currentProcessingTime()
          println("定时器注册时间：" + currentProcessingTime + " 定时器触发时间：" + ((currentProcessingTime / 60000 + 1) * 60000))
          ctx.timerService().registerProcessingTimeTimer((currentProcessingTime / 60000 + 1) * 60000)
          val currentCount: Int = value._2
          var historyCount: Integer = counter.value()
          if (historyCount == null) {
            historyCount = 0
          }
          historyCount += currentCount
          counter.update(historyCount)
          out.collect((value._1, historyCount))
        }

        override def onTimer(timestamp: Long,
                             ctx: KeyedProcessFunction[String, (String, Int), (String, Int)]#OnTimerContext, out: Collector[(String, Int)]): Unit = {
          val value: Integer = counter.value()
          val currentKey: String = ctx.getCurrentKey
          out.collect((currentKey, value))
        }
      })
      .print()

    env.execute()
  }
}
