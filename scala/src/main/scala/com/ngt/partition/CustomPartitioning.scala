package com.ngt.partition

import org.apache.flink.api.common.functions.{Partitioner, RichMapFunction}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @author ngt
 * @create 2021-02-05 21:13
 */
object CustomPartitioning {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)
    env.setParallelism(4)

    val mapDataStream: DataStream[(String, Int)] = lines.map(new RichMapFunction[String, (String, Int)] {
      override def map(value: String): (String, Int) = (value, getRuntimeContext.getIndexOfThisSubtask)
    }).setParallelism(4)

    mapDataStream.partitionCustom(new Partitioner[String] {
      override def partition(key: String, numPartitions: Int): Int = {
        var res = 0
        if ("spark".equals(key)) {
          res = 1
        } else if ("flink".equals(key)) {
          res = 2
        } else if ("hadoop".equals(key)) {
          res = 3
        }
        res
      }
    }, _._1)
      .addSink(new RichSinkFunction[(String, Int)] {
        override def invoke(value: (String, Int), context: SinkFunction.Context): Unit = {
          println(value._1 + " : " + value._2 + " -> " + getRuntimeContext().getIndexOfThisSubtask())
        }
      })
    env.execute()
  }
}

/*
    spark : 3 -> 1
    spark : 0 -> 1
    flink : 1 -> 2
    hadoop : 2 -> 3
    flink : 3 -> 2
    java : 0 -> 0
    scala : 1 -> 0
    hadoop : 2 -> 3
 */