package com.ngt.state

import org.apache.flink.api.common.functions.{FlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * @author ngt
 * @create 2021-02-09 5:46
 */
object MyKeyedStateDemo01 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)))
    env.enableCheckpointing(5000)

    val wordAndOne: DataStream[(String, Int)] = lines.flatMap(new FlatMapFunction[String, (String, Int)] {
      override def flatMap(value: String, out: Collector[(String, Int)]): Unit = {
        val words: Array[String] = value.split(" ")
        for (elem <- words) {
          if ("error".equals(elem)) {
            throw new RuntimeException("Exception")
          }
          out.collect((elem, 1))
        }
      }
    })

    // 不调用 sum，因为其会存在状态,自己实现状态存储， 使用 RichMapFunction 便于获取运行时上下文
    // 注意此方法不能实现状态的保存，无论是否启用 enableCheckpointing，因为该方法没有将数据持久化存储
    wordAndOne.keyBy(_._1)
      .map(new RichMapFunction[(String, Int), (String, Int)] {
        private val counter: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()

        override def map(value: (String, Int)): (String, Int) = {
          val word: String = value._1
          val count: Int = value._2
          var historyCount: Int = 0
          val maybeInt: Option[Int] = counter.get(word)
          if (maybeInt.isDefined) {
            historyCount = maybeInt.get
          }
          val sum: Int = historyCount + count
          counter.put(word, sum)
          (word, sum)
        }
      })
      .print()
    env.execute()
  }
}

/*
spark
saprk
flink flink
flink
hadoop
hadoop
hive

1> (spark,1)
8> (hadoop,1)
7> (flink,1)
1> (hive,1)
7> (flink,2)
7> (flink,3)
2> (saprk,1)
8> (hadoop,2)
 */