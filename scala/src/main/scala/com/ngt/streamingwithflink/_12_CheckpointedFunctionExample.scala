package com.ngt.streamingwithflink

import com.ngt.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import java.time.Duration
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
 * @author ngt
 * @create 2021-05-15 22:57
 */
object _12_CheckpointedFunctionExample {
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
      .flatMap(new HighTempCounter(10.0))
      .print()

    env.execute()
  }

}


// 分别利用键值分区状态和算子状态来统计每个键值分区和每个算子实例中超过阈值的温度数
class HighTempCounter(val threshold: Double)
  extends FlatMapFunction[SensorReading, (String, Long, Long)]
    with CheckpointedFunction {
  var opHighTempCnt: Long = 0

  var keyedCntState: ValueState[Long] = _
  var opCntState: ListState[Long] = _

  override def flatMap(value: SensorReading, out: Collector[(String, Long, Long)]): Unit = {
    if (value.temperature > threshold) {
      opHighTempCnt += 1 // 更新本地算子实例高温计数器
      val keyHighTempCnt: Long = keyedCntState.value() + 1 // 更新键值分区的高温计数器
      keyedCntState.update(keyHighTempCnt)
      out.collect((value.id, keyHighTempCnt, opHighTempCnt))
    }
  }

  // 生成检查点时调用
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    // 利用本地状态更新算子状态
    opCntState.clear()
    opCntState.add(opHighTempCnt)
  }

  // 启动任务的时候调用
  override def initializeState(context: FunctionInitializationContext): Unit = {
    // 初始化键值分区状态
    val keyCntDescriptor: ValueStateDescriptor[Long] = new ValueStateDescriptor[Long]("keyedCnt", classOf[Long])
    keyedCntState = context.getKeyedStateStore.getState(keyCntDescriptor)

    // 初始化算子状态
    val opCntDescriptor: ListStateDescriptor[Long] = new ListStateDescriptor[Long]("opCnt", classOf[Long])
    opCntState = context.getOperatorStateStore.getListState(opCntDescriptor)

    // 利用算子状态初始化本地的变量
    opHighTempCnt = opCntState.get().asScala.sum

  }
}