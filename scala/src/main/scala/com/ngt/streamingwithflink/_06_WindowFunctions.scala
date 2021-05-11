package com.ngt.streamingwithflink

import com.ngt.streamingwithflink.util._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import java.lang
import java.time.Duration
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

/**
 * @author ngt
 * @create 2021-05-11 16:09
 */
object _06_WindowFunctions {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setAutoWatermarkInterval(1000L)

    // 1. ReduceFunction 使用lambda表达式
    env.addSource(new SensorSource)
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forBoundedOutOfOrderness[SensorReading](Duration.ofSeconds(1))
        .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
          override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timestamp
        }))
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .reduce((r1, r2) => (r1._1, math.min(r1._2, r2._2)))
      .print()

    // 1. ReduceFunction 使用类
    env.addSource(new SensorSource)
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forBoundedOutOfOrderness[SensorReading](Duration.ofSeconds(1))
        .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
          override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timestamp
        }))
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .reduce(new MinTempFunction())
      .print()

    // 2. aggregate 求平均值
    env.addSource(new SensorSource)
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forBoundedOutOfOrderness[SensorReading](Duration.ofSeconds(1))
        .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
          override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timestamp
        }))
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .aggregate(new AvgTempFunction)
      .print()


    // 3. 全量聚合 HighAndLowTempProcessFunction 求最值
    env.addSource(new SensorSource)
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forBoundedOutOfOrderness[SensorReading](Duration.ofSeconds(1))
        .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
          override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timestamp
        }))
      .keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .process(new HighAndLowTempProcessFunction)
      .print()


    // 4. 增量聚合 HighAndLowTempProcessFunction 求最值
    env.addSource(new SensorSource)
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forBoundedOutOfOrderness[SensorReading](Duration.ofSeconds(1))
        .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
          override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timestamp
        }))
      .map(r => (r.id, r.temperature, r.temperature))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .reduce((r1: (String, Double, Double), r2: (String, Double, Double)) => {
        (r1._1, r1._2.min(r2._2), r1._3.max(r2._3))
      }, new AssignWindowEndProcessFunction)
      .print()

    env.execute()

  }
}


case class MinMaxTemp(id: String, min: Double, max: Double, endTs: Long)

class MinTempFunction extends ReduceFunction[(String, Double)] {
  override def reduce(value1: (String, Double), value2: (String, Double)): (String, Double) = {
    (value1._1, value1._2.min(value2._2))
  }
}

class AvgTempFunction extends AggregateFunction[(String, Double), (String, Double, Int), (String, Double)] {
  // 创建一个累加器类启动聚合
  override def createAccumulator(): (String, Double, Int) = ("", 0.0, 0)

  // 向累加器中添加一个输入元素并返回累加器
  override def add(value: (String, Double), accumulator: (String, Double, Int)): (String, Double, Int) =
    (value._1, value._2 + accumulator._2, accumulator._3 + 1)

  // 根据累加器计算并返回结果
  override def getResult(accumulator: (String, Double, Int)): (String, Double) =
    (accumulator._1, accumulator._2 / accumulator._3)

  //  合并两个累加器并返回合并结果
  override def merge(a: (String, Double, Int), b: (String, Double, Int)): (String, Double, Int) =
    (a._1, a._2 + b._2, a._3 + b._3)
}


// 注意此处需要 import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
class HighAndLowTempProcessFunction
  extends ProcessWindowFunction[SensorReading, MinMaxTemp, String, TimeWindow] {
  override def process(key: String,
                       context: Context,
                       elements: Iterable[SensorReading],
                       out: Collector[MinMaxTemp]): Unit = {

    val temps: Iterable[Double] = elements.map(_.temperature)
    val windowEnd: Long = context.window.getEnd
    out.collect(MinMaxTemp(key, temps.min, temps.max, windowEnd))
  }
}

class AssignWindowEndProcessFunction
  extends ProcessWindowFunction[(String, Double, Double), MinMaxTemp, String, TimeWindow] {
  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, Double, Double)],
                       out: Collector[MinMaxTemp]): Unit = {
    val minMax: (String, Double, Double) = elements.head
    val windowEnd: Long = context.window.getEnd
    out.collect(MinMaxTemp(key, minMax._2, minMax._3, windowEnd))
  }
}