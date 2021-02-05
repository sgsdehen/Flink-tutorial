package com.ngt.function

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author ngt
 * @create 2021-02-05 17:34
 */
object WindowProcessFunctionDemo {
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
      .process(new ProcessWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
        override def process(key: String,
                             context: Context, elements: Iterable[(String, Int)],
                             out: Collector[(String, Int)]): Unit = {
          for (elem <- elements) {
            out.collect(elem)
          }
        }
      }).print()

    env.execute()
  }
}
