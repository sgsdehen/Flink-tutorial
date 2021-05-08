package com.ngt.source

import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.NumberSequenceIterator

import java.lang

/**
 * @author ngt
 * @create 2021-01-26 16:21
 */
object CollectionSourceDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    println("StreamExecutionEnvironmentParallelism: " + env.getParallelism)

    // 1. fromElements 非并行有限数据的Source
    val elements: DataStream[String] = env.fromElements("flink", "hadoop", "spark")
    println("StreamExecutionEnvironmentParallelism: " + env.getParallelism);

    // 2. fromCollection 非并行有限数据的Source，接收Collection类型的数据
    val collection: DataStream[String] = env.fromCollection(List("flink", "hadoop", "spark"))
    println("collectionParallelism: " + collection.parallelism);

    // 3. fromParallelCollection 并行的Source，可以设置并行度
    // 第一个是继承SplittableIterator的实现类的迭代器，第二个是迭代器中数据的类型
    val parallelCollection: DataStream[lang.Long] = env.fromParallelCollection(new NumberSequenceIterator(1L, 100L)).setParallelism(4)
    println("parallelCollectionParallelism: " + parallelCollection.parallelism)

    // 4. generateSequence 并行的Source该方法需要传入两个long类型的参数，第一个是起始值，第二个是结束值
    val generateSequence: DataStream[Long] = env.generateSequence(1, 100).setParallelism(4)
    println("generateSequenceParallelism: " + generateSequence.parallelism)

    // 5. fromSequence, 用于替换 generateSequence
    val fromSequence: DataStream[Long] = env.fromSequence(1, 100).setParallelism(4)
    println("fromSequenceParallelism: " + fromSequence.parallelism)

    fromSequence.print()
    env.execute("CollectionSourceDemo")

  }

}
