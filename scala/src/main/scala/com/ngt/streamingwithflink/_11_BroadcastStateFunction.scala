package com.ngt.streamingwithflink

import com.ngt.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * @author ngt
 * @create 2021-05-15 22:10
 */
object _11_BroadcastStateFunction {
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

    val thresholds: DataStream[ThresholdUpdate] = env.fromElements(
      ThresholdUpdate("sensor_1", 5.0d),
      ThresholdUpdate("sensor_2", 0.9d),
      ThresholdUpdate("sensor_3", 0.5d),
      ThresholdUpdate("sensor_1", 1.2d), // update threshold for sensor_1
      ThresholdUpdate("sensor_3", 0.0d)) // disable threshold for sensor_3

    val broadcastStateDescriptor: MapStateDescriptor[String, Double] =
      new MapStateDescriptor[String, Double]("thresholds", classOf[String], classOf[Double])

    val broadcastThresholds: BroadcastStream[ThresholdUpdate] =
      thresholds.broadcast(broadcastStateDescriptor)

    sensorData
      .keyBy(_.id)
      .connect(broadcastThresholds)
      .process(new UpdatableTemperatureAlertFunction)
      .print()

    env.execute()

  }
}

case class ThresholdUpdate(id: String, threshold: Double)

class UpdatableTemperatureAlertFunction()
  extends KeyedBroadcastProcessFunction[String, SensorReading, ThresholdUpdate, (String, Double, Double)] {
  private lazy val thresholdStateDescriptor: MapStateDescriptor[String, Double] =
    new MapStateDescriptor[String, Double]("thresholds", classOf[String], classOf[Double])

  private lazy val lastTempState: ValueState[Double] =
    getRuntimeContext.getState[Double](new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  override def processElement(value: SensorReading,
                              ctx: KeyedBroadcastProcessFunction[String, SensorReading, ThresholdUpdate, (String, Double, Double)]#ReadOnlyContext,
                              out: Collector[(String, Double, Double)]): Unit = {
    val thresholds: ReadOnlyBroadcastState[String, Double] = ctx.getBroadcastState(thresholdStateDescriptor)
    if (thresholds.contains(value.id)) {
      val sensorThreshold: Double = thresholds.get(value.id)
      val lastTemp: Double = lastTempState.value()
      val tempDiff: Double = (value.temperature - lastTemp).abs

      if (tempDiff > sensorThreshold) {
        out.collect((value.id, value.temperature, tempDiff))
      }
    }
  }

  override def processBroadcastElement(value: ThresholdUpdate,
                                       ctx: KeyedBroadcastProcessFunction[String, SensorReading, ThresholdUpdate, (String, Double, Double)]#Context,
                                       out: Collector[(String, Double, Double)]): Unit = {
    val thresholds: BroadcastState[String, Double] = ctx.getBroadcastState(thresholdStateDescriptor)
    if (value.threshold != 0.0d) {
      thresholds.put(value.id, value.threshold)
    } else {
      thresholds.remove(value.id)
    }
  }
}