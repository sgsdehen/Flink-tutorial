package com.ngt.state

import org.apache.flink.api.common.functions.{FlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author ngt
 * @create 2021-02-09 5:34
 */
object MyKeyedStateDemo00 {
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

    // 因为此处只是保存了一个value没有保存 key，所以会将一个subtask中的所有进行累加操作，而不是分成
    // 不同的 key
    wordAndOne.keyBy(_._1)
      .map(new RichMapFunction[(String, Int), (String, Int)] {
        private var counter: Int = _

        override def map(value: (String, Int)): (String, Int) = {
          val currentCount: Int = value._2
          counter += currentCount
          (value._1, counter)
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
2> (saprk,1)
7> (flink,1)
7> (flink,2)
7> (flink,3)
8> (hadoop,1)
8> (hadoop,2)
1> (hive,2)  hive会接着 (spark,1)进行累加所以是 (hive,2)
 */