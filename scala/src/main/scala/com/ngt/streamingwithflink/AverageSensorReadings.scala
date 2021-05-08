package com.ngt.streamingwithflink


import com.ngt.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.eventtime.{BoundedOutOfOrdernessWatermarks, SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * @author ngt
 * @create 2021-05-07 21:45
 */
object AverageSensorReadings {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // In Flink 1.12 the default stream time characteristic has been changed to TimeCharacteristic.EventTime,
    // 每一秒生成一次水位线
    // env.getConfig.setAutoWatermarkInterval(1000L)

    val sensorData: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forBoundedOutOfOrderness[SensorReading](Duration.ZERO)
        .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
          override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timestamp
        }))

    // 输入元素的时间戳如果是单调递增可以使用该简便方法
    //     .assignAscendingTimestamps(_.timestamp)


    // 输入元素的时间戳如果是单调递增也可以使用该方法
    //      .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps()
    //        .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
    //          override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp
    //        }))

    // 输入元素的时间戳存在乱序，使用该方法
    //    .assignTimestampsAndWatermarks(WatermarkStrategy
    //      .forBoundedOutOfOrderness[SensorReading](Duration.ofSeconds(1))
    //      .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
    //        override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timestamp
    //      }))

    //.assignTimestampsAndWatermarks(new SensorTimeAssigner)  该方法已经过时


    val avgTemp: DataStream[SensorReading] = sensorData
      .map(r =>
        SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0)))
      .keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(1)))
      .apply(new TemperatureAverager)

    avgTemp.print()

    env.execute("Compute average sensor temperature")
  }
}

class TemperatureAverager extends WindowFunction[SensorReading, SensorReading, String, TimeWindow] {

  override def apply(
                      sensorId: String,
                      window: TimeWindow,
                      vals: Iterable[SensorReading],
                      out: Collector[SensorReading]): Unit = {

    val (cnt, sum) = vals.foldLeft((0, 0.0))((c, r) => (c._1 + 1, c._2 + r.temperature))
    val avgTemp = sum / cnt

    out.collect(SensorReading(sensorId, window.getEnd, avgTemp))
  }
}

