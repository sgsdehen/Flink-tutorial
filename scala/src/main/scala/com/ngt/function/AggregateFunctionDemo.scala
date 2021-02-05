package com.ngt.function

import org.apache.flink.api.common.functions.{AggregateFunction, FlatMapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @author ngt
 * @create 2021-02-05 13:52
 */
object AggregateFunctionDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val words: DataStream[(String, Long)] = lines.flatMap(new FlatMapFunction[String, (String, Long)]() {
      override def flatMap(value: String, out: Collector[(String, Long)]): Unit = {
        val strings: Array[String] = value.split(" ")
        out.collect(strings(0), strings(1).toLong)
      }
    })

    words.print()
    words.keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .aggregate(new AverageAggregate)
      .print()
    env.execute()
  }

  class AverageAggregate() extends AggregateFunction[(String, Long), (Long, Long), (String, Double)] {
    private var key: String = _

    override def createAccumulator(): (Long, Long) = (0L, 0L)

    override def add(value: (String, Long), accumulator: (Long, Long)): (Long, Long) = {
      key = value._1
      (accumulator._1 + value._2, accumulator._2 + 1L)
    }

    override def getResult(accumulator: (Long, Long)): (String, Double) = (key, accumulator._1 / accumulator._2.toDouble)

    // 非 SessionWindow，可以不实现
    override def merge(a: (Long, Long), b: (Long, Long)): (Long, Long) = {
      (a._1 + b._1, a._2 + b._2)
    }
  }

}
/*
flink 1
flink 2
flink 3
flink 4
spark 1
hadoop 2
spark 3
hadoop 21

 */