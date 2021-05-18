package com.ngt.streamingwithflink

import com.ngt.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

/**
 * @author ngt
 * @create 2021-05-11 10:09
 * 为不同的id设置不同的转发时间
 */
object _05_CoProcessFunctionTimers {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setAutoWatermarkInterval(1000L)
    val filterSwitches: DataStream[(String, Long)] = env
      .fromCollection(Seq(
        ("sensor_2", 5 * 1000L), // 转发 5 秒之后停止
        ("sensor_7", 10 * 1000L)) // 转发 10 秒之后停止
      )

    val readings: DataStream[SensorReading] = env.addSource(new SensorSource)

    readings
      .connect(filterSwitches)
      .keyBy(_.id, _._1)
      .process(new ReadingFilter)
      .print()

    env.execute()

  }
}

class ReadingFilter extends CoProcessFunction[SensorReading, (String, Long), SensorReading] {

  private lazy val forwardingEnabled: ValueState[Boolean] =
    getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("filterSwitch", classOf[Boolean]))


  private lazy val disableTimer: ValueState[Long] =
    getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", classOf[Long]))

  override def processElement1(value: SensorReading,
                               ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                               out: Collector[SensorReading]): Unit = {
    if (forwardingEnabled.value()) {
      out.collect(value)
    }
  }

  override def processElement2(value: (String, Long),
                               ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                               out: Collector[SensorReading]): Unit = {

    forwardingEnabled.update(true) // 开启读数转发

    val curTimerTimestamp: Long = disableTimer.value()
    val timerTimestamp: Long = ctx.timerService().currentProcessingTime() + value._2

    if (timerTimestamp > curTimerTimestamp) {
      // 移除当前计时器创建一个新的计时器
      ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
      ctx.timerService().registerProcessingTimeTimer(timerTimestamp)
      disableTimer.update(timerTimestamp)
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext,
                       out: Collector[SensorReading]): Unit = {
    // 移除所有状态，默认情况下转发开关关闭
    forwardingEnabled.clear()
    disableTimer.clear()
  }
}