package com.ngt.streamingwithflink

import com.ngt.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * @author ngt
 * @create 2021-05-11 8:44
 */
object _04_SideOutputs {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setAutoWatermarkInterval(1000L)

    env.getCheckpointConfig.setCheckpointInterval(10 * 1000L)

    val monitoredReadings: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forBoundedOutOfOrderness[SensorReading](Duration.ofSeconds(5))
        .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
          override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timestamp
        }))
      .process(new FreezingMonitor)


    // 获取副输出流。注意此处的id必须要和 FreezingMonitor 中的保持一致
    monitoredReadings
      .getSideOutput(new OutputTag[String]("freezing-alarms"))
      .print()

    env.execute()
  }

}

class FreezingMonitor extends ProcessFunction[SensorReading, SensorReading] {
  // 定义副输出流
  private val freezingAlarmOutput: OutputTag[String] =
    new OutputTag[String]("freezing-alarms")

  override def processElement(value: SensorReading,
                              ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                              out: Collector[SensorReading]): Unit = {
    if (value.temperature < 32.0) {
      ctx.output(freezingAlarmOutput, s"Freezing Alarm for ${value.id} temperature is ${value.temperature}")
    }
    out.collect(value)
  }
}
