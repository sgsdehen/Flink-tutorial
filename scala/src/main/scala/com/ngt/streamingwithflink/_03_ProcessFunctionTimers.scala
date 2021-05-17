package com.ngt.streamingwithflink

import com.ngt.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * @author ngt
 * @create 2021-05-10 21:50
 *         如果传感器在处理时间 1 秒内持续增加
 */
object _03_ProcessFunctionTimers {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val readings: DataStream[SensorReading] = env
      .addSource(new SensorSource)

    readings
      .keyBy(_.id)
      .process(new TempIncreaseAlertFunction)
      .print()

    env.execute()
  }
}

class TempIncreaseAlertFunction extends KeyedProcessFunction[String, SensorReading, String] {
  // 存储最近一次的温度值
  private lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("lasttemp", classOf[Double])
  )
  // 存储当前活动计时器的时间戳
  private lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("currentTimer", classOf[Long])
  )


  override def processElement(i: SensorReading,
                              context: KeyedProcessFunction[String, SensorReading, String]#Context,
                              collector: Collector[String]): Unit = {
    val prevTemp: Double = lastTemp.value() //获取前一个温度
    lastTemp.update(i.temperature) // 更新最近一次温度
    val curTimerTimestamap: Long = currentTimer.value()

    if (prevTemp == 0 || prevTemp > i.temperature) {
      //温度下降删除当前计时器
      context.timerService().deleteProcessingTimeTimer(curTimerTimestamap)
      currentTimer.clear()
    } else if (i.temperature > prevTemp && curTimerTimestamap == 0) {
      // 温度升高且没有设置计时器
      context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + 1000L)
      currentTimer.update(context.timerService().currentProcessingTime() + 1000L)
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    out.collect("Sensor " + ctx.getCurrentKey + "温度持续上升1秒钟")
  }


}
