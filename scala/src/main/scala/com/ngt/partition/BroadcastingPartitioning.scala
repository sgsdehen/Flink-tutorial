package com.ngt.partition

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @author ngt
 * @create 2021-02-05 21:03
 * 广播：将上游的每个数据发送到下游的所有 Task
 */
object BroadcastingPartitioning {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)
    env.setParallelism(4)

    val mapDataStream: DataStream[String] = lines.map(new RichMapFunction[String, String] {
      override def map(value: String): String = {
        value + " : " + getRuntimeContext.getIndexOfThisSubtask
      }
    }).setParallelism(1)

    val broadcast: DataStream[String] = mapDataStream.broadcast

    broadcast.addSink(new RichSinkFunction[String] {
      override def invoke(value: String, context: SinkFunction.Context): Unit = {
        println(value + " -> " + getRuntimeContext().getIndexOfThisSubtask())
      }
    })
    env.execute()
  }
}
/*
   将上游的每个数据发送到下游的所有 Task
    aaaa ： 0 -> 3
    aaaa ： 0 -> 2
    aaaa ： 0 -> 0
    aaaa ： 0 -> 1
    bbbb ： 0 -> 3
    bbbb ： 0 -> 2
    bbbb ： 0 -> 0
    bbbb ： 0 -> 1
    gggg ： 0 -> 3
    gggg ： 0 -> 2
    gggg ： 0 -> 1
    gggg ： 0 -> 0
 */