package com.ngt.streamingwithflink

import com.ngt.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api. environment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration
import java.util.Collections

/**
 * @author ngt
 * @create 2021-05-12 18:17
 */
object _07_CustomWindows {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setAutoWatermarkInterval(1000L)

    env.addSource(new SensorSource)
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forBoundedOutOfOrderness[SensorReading](Duration.ofSeconds(1))
        .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
          override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timestamp
        }))
      .keyBy(_.id)
      .window(new ThirtySecondsWindows)       // 窗口分配器
      .trigger(new OneSecondIntervalTrigger)  // 可选项：指定分类器
      .process(new CountFunction)             // 指定窗口函数
      .print()

    env.execute()

  }

}


// 按照每30秒滚动窗口进行分组的自定义窗口
class ThirtySecondsWindows extends WindowAssigner[Object, TimeWindow] {
  val windowSize: Long = 30 * 1000L

  override def assignWindows(element: Object,
                             timestamp: Long,
                             context: WindowAssigner.WindowAssignerContext):java.util.List[TimeWindow] = {
    val startTime: Long = timestamp - (timestamp % windowSize)
    val endTime: Long = startTime + windowSize
    // 发出相应的窗口
    Collections.singletonList(new TimeWindow(startTime, endTime))
  }


  override def getDefaultTrigger(env: environment.StreamExecutionEnvironment): Trigger[Object, TimeWindow] = {
    EventTimeTrigger.create()
  }

  override def getWindowSerializer(executionConfig: ExecutionConfig): TypeSerializer[TimeWindow] = {
    new TimeWindow.Serializer
  }

  override def isEventTime: Boolean = true
}


// 可提前触发的触发器，触发周期不小于一秒

class OneSecondIntervalTrigger extends Trigger[SensorReading, TimeWindow] {
  override def onElement(element: SensorReading,
                         timestamp: Long, window: TimeWindow,
                         ctx: Trigger.TriggerContext): TriggerResult = {
    // 获取一个作用域为触发器键值和当前窗口的状态对象
    val firstSeen: ValueState[Boolean] = ctx.getPartitionedState(
      new ValueStateDescriptor[Boolean]("firstSeen", classOf[Boolean]))
    if (!firstSeen.value()) {
      // 得到下一个整点的时间戳
      val t: Long = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
      ctx.registerEventTimeTimer(t)

      ctx.registerEventTimeTimer(window.getEnd)
      firstSeen.update(true)
    }
    TriggerResult.CONTINUE
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE // 不使用处理时间计时器
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    // 进行最终计算并清除窗口状态
    if (time == window.getEnd) {
      TriggerResult.FIRE_AND_PURGE  // 先计算再删除
    } else {
      // 注册下一个用于提前触发的计时器
      val t: Long = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
      if (t < window.getEnd) {
        ctx.registerEventTimeTimer(t)
      }
      TriggerResult.FIRE   // 执行计算
    }
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    val firstSeen: ValueState[Boolean] = ctx.getPartitionedState(
      new ValueStateDescriptor[Boolean]("firstSeen", classOf[Boolean]))
    firstSeen.clear()
  }
}

class CountFunction
  extends ProcessWindowFunction[SensorReading, (String, Long, Long, Int), String, TimeWindow] {
  override def process(key: String,
                       context: Context,
                       elements: Iterable[SensorReading],
                       out: Collector[(String, Long, Long, Int)]): Unit = {
    val cnt: Int = elements.count(_ => true)
    val evalTime: Long = context.currentWatermark
    out.collect((key, context.window.getEnd, evalTime, cnt))
  }
}
