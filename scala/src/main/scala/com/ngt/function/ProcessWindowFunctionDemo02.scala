package com.ngt.function

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author ngt
 * @create 2021-02-05 17:00
 */
object ProcessWindowFunctionDemo02 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val timeAndCount: DataStream[(String, Int)] = lines
      .assignAscendingTimestamps(_.split(",")(0).toLong)
      .map(data => {
        val split: Array[String] = data.split(",")
        (split(1), split(2).toInt)
      })

    timeAndCount.keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .aggregate(new MyAggFunc, new MyWindowFunction)
      .print()

    env.execute()
  }

  class MyAggFunc extends AggregateFunction[(String, Int), Int, Int] {
    override def createAccumulator(): Int = 0

    override def add(value: (String, Int), accumulator: Int): Int = value._2 + createAccumulator()

    override def getResult(accumulator: Int): Int = accumulator

    // 非 SessionWindow，可以不实现
    override def merge(a: Int, b: Int): Int = ???
  }

  class MyWindowFunction extends ProcessWindowFunction[Int, (String, Int), String, TimeWindow] {
    lazy private val sumState: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("wc", classOf[Int]))

    override def process(key: String, context: Context, elements: Iterable[Int], out: Collector[(String, Int)]): Unit = {
      var historyCount: Int = sumState.value()
      if (historyCount == null) {
        historyCount = 0
      }
      val windowCount: Integer = elements.iterator.next
      sumState.update(historyCount + windowCount)
      out.collect((key, windowCount + historyCount))
    }
  }

}
