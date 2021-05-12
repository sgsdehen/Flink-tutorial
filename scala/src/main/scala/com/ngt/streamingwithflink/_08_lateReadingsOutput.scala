package com.ngt.streamingwithflink

import com.ngt.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import java.time.Duration
import scala.util.Random

/**
 * @author ngt
 * @create 2021-05-12 19:08
 */
object _08_lateReadingsOutput {
  val lateReadingsOutput: OutputTag[SensorReading] =
    new OutputTag[SensorReading]("late-readings")

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setAutoWatermarkInterval(500L)

    val outOfOrderReadings: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .map(new TimestampShuffler(7 * 1000))
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forBoundedOutOfOrderness[SensorReading](Duration.ofSeconds(5))
        .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
          override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timestamp
        }))

    // 1. 使用ProcessFunction过滤出延迟读数(到侧输出)
    //filterLateReadings(outOfOrderReadings)
    // 2. 将窗口操作符中的迟到读数重定向到侧输出
    //sideOutputLateEventsWindow(outOfOrderReadings)
    // 3. 在窗口操作符中接收到延迟读数时更新结果
    updateForLateEventsWindow(outOfOrderReadings)
    env.execute()
  }

  // 过滤迟发读数到侧输出并打印实时和延迟的流。
  def filterLateReadings(readings: DataStream[SensorReading]): Unit = {
    val filteredReadings: DataStream[SensorReading] = readings.process(new LateReadingsFilter)

    val lateReadings: DataStream[SensorReading] = filteredReadings.getSideOutput(lateReadingsOutput)

    lateReadings.print()

    lateReadings
      .map(r => "*** late reading *** " + r.id)
      .print()
  }


  //计数每个滚动窗口的读数，并向侧输出发出晚读数。打印结果和延迟事件。

  def sideOutputLateEventsWindow(readings: DataStream[SensorReading]): Unit = {
    val countPer10Secs: DataStream[(String, Long, Int)] = readings
      .keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .sideOutputLateData(lateReadingsOutput)
      .process(new ProcessWindowFunction[SensorReading, (String, Long, Int), String, TimeWindow] {
        override def process(key: String,
                             context: Context,
                             elements: Iterable[SensorReading],
                             out: Collector[(String, Long, Int)]): Unit = {
          val cnt: Int = elements.count(_ => true)
          out.collect((key, context.window.getEnd, cnt))
        }
      })

    countPer10Secs
      .getSideOutput(lateReadingsOutput)
      .map(r => "*** late reading *** " + r.id)
      .print()

    countPer10Secs.print()
  }

  def updateForLateEventsWindow(readings: DataStream[SensorReading]): Unit = {

    val countPer10Secs: DataStream[(String, Long, Int, String)] = readings
      .keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .allowedLateness(Time.seconds(5))
      .process(new UpdatingWindowCountFunction)

    countPer10Secs
      .print()
  }
}


class LateReadingsFilter extends ProcessFunction[SensorReading, SensorReading] {
  override def processElement(
                               r: SensorReading,
                               ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                               out: Collector[SensorReading]): Unit = {

    if (r.timestamp < ctx.timerService().currentWatermark()) {
      ctx.output(_08_lateReadingsOutput.lateReadingsOutput, r)
    } else {
      out.collect(r)
    }
  }
}


class UpdatingWindowCountFunction extends ProcessWindowFunction[SensorReading, (String, Long, Int, String), String, TimeWindow] {
  override def process(key: String,
                       context: Context,
                       elements: Iterable[SensorReading],
                       out: Collector[(String, Long, Int, String)]): Unit = {

    val cnt: Int = elements.count(_ => true)
    val isUpdate: ValueState[Boolean] =
      context.windowState.getState(new ValueStateDescriptor[Boolean]("isUpdate", classOf[Boolean]))

    if (!isUpdate.value()) {
      out.collect((key, context.window.getEnd, cnt, "first"))
      isUpdate.update(true)
    } else {
      out.collect((key, context.window.getEnd, cnt, "update"))
    }
  }
}


class TimestampShuffler(maxRandomOffset: Int) extends MapFunction[SensorReading, SensorReading] {
  lazy val rand: Random = new Random()

  override def map(value: SensorReading): SensorReading = {
    val shuffleTs: Long = value.timestamp + rand.nextInt(maxRandomOffset)
    SensorReading(value.id, shuffleTs, value.temperature)
  }
}