package com.ngt.function

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author ngt
 * @create 2021-02-05 16:39
 */
object ProcessWindowFunctionDemo01 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val windowedStream: WindowedStream[(String, Int), String, TimeWindow] = lines.map(data => {
      val split: Array[String] = data.split(",")
      (split(0).toLong, split(1), split(2).toInt)
    })
      .assignAscendingTimestamps(_._1)
      .map(data => (data._2, data._3))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))

    windowedStream.reduce(new MyReduceFunction, new MyWindowFunction)
      .print()
    env.execute()
  }

  class MyReduceFunction() extends ReduceFunction[(String, Int)] {
    override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
      (value1._1, value1._2 + value2._2)
    }
  }

  class MyWindowFunction extends ProcessWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
    lazy private val sumState: ValueState[Int] =
      getRuntimeContext().getState(new ValueStateDescriptor[Int]("wc", classOf[Int]))

    override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
      var historyCount: Int = sumState.value()
      if (historyCount == null) {
        historyCount = 0
      }
      val tp: (String, Int) = elements.iterator.next()
      sumState.update(tp._2 + historyCount)
      out.collect(tp._1, tp._2 + historyCount)
    }
  }

}
