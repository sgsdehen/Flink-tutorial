package com.ngt.streamingwithflink

import com.ngt.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.time.Duration


/**
 * @author ngt
 * @create 2021-05-10 11:43
 *         水位线生成
 */
object _02_WatermarkGeneration {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setAutoWatermarkInterval(1000L)

    val readings: DataStream[SensorReading] = env
      .addSource(new SensorSource)


    // 1. 自定义 WatermarkGenerator 实现
    readings
      .assignTimestampsAndWatermarks(new PeriodicWS)
      .print()


    // 2. 时间戳单调递增
    readings
      .assignAscendingTimestamps(_.timestamp)
      .print()

    readings
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forMonotonousTimestamps[SensorReading]()
        .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
          override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timestamp
        }))


    // 3. 提前知道最大延迟
    readings.assignTimestampsAndWatermarks(WatermarkStrategy
      .forBoundedOutOfOrderness[SensorReading](Duration.ofSeconds(5))
      .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
        override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timestamp
      }))


    // 4. 处理空闲数据源
    readings.assignTimestampsAndWatermarks(WatermarkStrategy
      .forBoundedOutOfOrderness[SensorReading](Duration.ofSeconds(5))
      .withIdleness(Duration.ofMinutes(1))) // 设置空闲超时时间

    env.execute()


  }

}


// 此处应该有可完美的实现方式
class PeriodicWS extends WatermarkStrategy[SensorReading] {
  override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[SensorReading] = new PeriodicAssigner()
}

class PunctuatedWS extends WatermarkStrategy[SensorReading] {
  override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[SensorReading] = new PunctuatedAssigner()
}


// 周期性水位线分配器
class PeriodicAssigner extends WatermarkGenerator[SensorReading] {
  val maxOutOfOrderness: Long = 60 * 1000

  var currentMaxTimestamp: Long = _

  override def onEvent(event: SensorReading, eventTimestamp: Long, output: WatermarkOutput): Unit = {
    currentMaxTimestamp = math.max(currentMaxTimestamp, event.timestamp)
  }

  override def onPeriodicEmit(output: WatermarkOutput): Unit = {
    output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1))
  }
}

// 定点水位分配器
class PunctuatedAssigner extends WatermarkGenerator[SensorReading] {
  val maxOutOfOrderness: Long = 60 * 1000

  override def onEvent(event: SensorReading, eventTimestamp: Long, output: WatermarkOutput): Unit = {
    if (event.id == "sensor_1") {
      output.emitWatermark(new Watermark(event.timestamp - maxOutOfOrderness - 1))
    }
  }

  override def onPeriodicEmit(output: WatermarkOutput): Unit = ??? // 无需实现
}

