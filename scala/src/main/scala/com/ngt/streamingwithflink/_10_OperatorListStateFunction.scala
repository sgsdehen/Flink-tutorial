package com.ngt.streamingwithflink

import com.ngt.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import java.lang
import java.time.Duration
import scala.collection.JavaConverters._

/**
 * @author ngt
 * @create 2021-05-15 21:27
 */
object _10_OperatorListStateFunction {
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
      .flatMap(new HighTempCounterOpState(120.0))
      .print()

    env.execute()

  }
}


class HighTempCounterOpState(val threshold: Double)
  extends RichFlatMapFunction[SensorReading, (Int, Long)]
    with ListCheckpointed[java.lang.Long] {
  private lazy val subtaskIndex: Int = getRuntimeContext.getIndexOfThisSubtask
  private var highTempCnt = 0L

  override def flatMap(value: SensorReading, out: Collector[(Int, Long)]): Unit = {
    if (value.temperature > threshold) {
      highTempCnt += 1
      out.collect((subtaskIndex, highTempCnt))
    }
  }

  // 有状态函数生成检查点的时候调用
  override def snapshotState(checkpointId: Long, timestamp: Long): java.util.List[java.lang.Long] = {
    java.util.Collections.singletonList(highTempCnt)
  }


  // 初始化函数状态时调用
  override def restoreState(state: java.util.List[lang.Long]): Unit = {
    highTempCnt = 0
    for (elem <- state.asScala) {
      highTempCnt += elem
    }
  }
}