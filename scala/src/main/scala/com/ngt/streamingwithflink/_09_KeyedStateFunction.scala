package com.ngt.streamingwithflink

import com.ngt.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.scala._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * @author ngt
 * @create 2021-05-15 20:57
 *         检测温度的测量值是否超过阈值
 */
object _09_KeyedStateFunction {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setAutoWatermarkInterval(500L)

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
      .flatMap(new TemperatureAlertFunction(1.7))
      .print()

    // 利用flatMapState实现只有一个键值分区的ValueState的FlatMap
    sensorData
      .keyBy(_.id)
      .flatMapWithState[(String, Double, Double), Double] {
        case (in: SensorReading, None) =>
          (List.empty, Some(in.temperature))
        case (r: SensorReading, lastTemp: Some[Double]) =>
          val tempDiff: Double = (r.temperature - lastTemp.get).abs
          if (tempDiff > 1.7) {
            (List((r.id, r.temperature, tempDiff)), Some(r.temperature))
          } else {
            (List.empty, Some(r.temperature))
          }
      }
    env.execute("Generate Temperature Alerts")
  }

}

// 使用flatMap是因为可能没有输出
class TemperatureAlertFunction(val threshold: Double)
  extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
  private lazy val lastTempState: ValueState[Double] =
    getRuntimeContext.getState[Double](new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 从状态中获取上一次的温度
    val lastTemp: Double = lastTempState.value()
    val tempDiff: Double = (value.temperature - lastTemp).abs

    if (tempDiff > threshold) {
      out.collect((value.id, value.temperature, tempDiff))
    }

    this.lastTempState.update(value.temperature)
  }
}
