package com.ngt.partition

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @author ngt
 * @create 2021-02-05 23:11
 */
object RandomPartitioning {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)
    env.setParallelism(4)

    val mapDataStream: DataStream[String] = lines.map(new RichMapFunction[String, String] {
      override def map(value: String): String = value + " ：" + getRuntimeContext().getIndexOfThisSubtask()
    }).setParallelism(4)
    val random: DataStream[String] = mapDataStream.shuffle
    random.addSink(new RichSinkFunction[String] {
      override def invoke(value: String, context: SinkFunction.Context): Unit = {
        println(value + " -> " + getRuntimeContext().getIndexOfThisSubtask())
      }
    })
    env.execute()
  }
}
