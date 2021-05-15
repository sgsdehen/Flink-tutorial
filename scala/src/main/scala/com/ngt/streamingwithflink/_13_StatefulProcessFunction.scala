package com.ngt.streamingwithflink

import com.ngt.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

import java.time.Duration

/**
 * @author ngt
 * @create 2021-05-15 23:20
 */
object _13_StatefulProcessFunction {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setAutoWatermarkInterval(1000L)

    env.getCheckpointConfig.setCheckpointInterval(10 * 1000L)

    val sensorData: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .map(new TimestampShuffler(7 * 1000))
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forBoundedOutOfOrderness[SensorReading](Duration.ofSeconds(5))
        .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
          override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timestamp
        }))

    sensorData
      .keyBy(_.id)
      .process(new SelfCleaningTemperatureAlertFunction(4.0))
      .print()

    env.execute("Generate Temperature Alerts")
  }
}

class SelfCleaningTemperatureAlertFunction(val threshold: Double)
  extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {

  private lazy val lastTempState: ValueState[Double] =
    getRuntimeContext.getState[Double](new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  private lazy val lastTimerState: ValueState[Long] =
    getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("timestampState", classOf[Long]))

  override def processElement(value: SensorReading,
                              ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context,
                              out: Collector[(String, Double, Double)]): Unit = {
    val newTimer: Long = ctx.timestamp() + (3600 * 1000L)
    val curTimer: Long = lastTimerState.value()

    ctx.timerService().deleteEventTimeTimer(curTimer)
    ctx.timerService().registerEventTimeTimer(newTimer)

    val lastTemp: Double = lastTempState.value()

    val tempDiff = (value.temperature - lastTemp).abs
    if (tempDiff > threshold) {
      out.collect((value.id, value.temperature, tempDiff))
    }
    lastTempState.update(value.temperature)
  }

  // 超过一小时未接收该键值就删除
  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#OnTimerContext,
                       out: Collector[(String, Double, Double)]): Unit = {
    lastTempState.clear()
    lastTimerState.clear()
  }
}
