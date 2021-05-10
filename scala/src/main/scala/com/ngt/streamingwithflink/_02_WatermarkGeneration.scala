package com.ngt.streamingwithflink

import com.ngt.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.eventtime.{Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}

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

    val assigner: PeriodicAssigner = new PeriodicAssigner
    readings
      .assignTimestampsAndWatermarks(new re)
      .print()

    env.execute()


  }

}



class re extends WatermarkStrategy[SensorReading]{
  override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[SensorReading] = new PeriodicAssigner()
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


//class PeriodicAssigner2 extends AssignerWithPeriodicWatermarks[SensorReading] {
//
//  // 1 min in ms
//  val bound: Long = 60 * 1000
//  // the maximum observed timestamp
//  var maxTs: Long = Long.MinValue
//
//  override def getCurrentWatermark: Watermark = {
//    new Watermark(maxTs - bound)
//  }
//
//  override def extractTimestamp(r: SensorReading, previousTS: Long): Long = {
//    // update maximum timestamp
//    maxTs = maxTs.max(r.timestamp)
//    // return record timestamp
//    r.timestamp
//  }
//}
//
//class PunctuatedAssigner2 extends AssignerWithPunctuatedWatermarks[SensorReading] {
//
//  // 1 min in ms
//  val bound: Long = 60 * 1000
//
//  override def checkAndGetNextWatermark(r: SensorReading, extractedTS: Long): Watermark = {
//    if (r.id == "sensor_1") {
//      // emit watermark if reading is from sensor_1
//      new Watermark(extractedTS - bound)
//    } else {
//      // do not emit a watermark
//      null
//    }
//  }
//
//  override def extractTimestamp(r: SensorReading, previousTS: Long): Long = {
//    // assign record timestamp
//    r.timestamp
//  }
//}
