package com.ngt.state

import org.apache.flink.api.common.functions.{FlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author ngt
 * @create 2021-02-09 9:23
 */
object KeyedStateTTLDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    env.enableCheckpointing(5000)
    // 未设置重启策略的时候程序出现异常就会退出，设置重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)))

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

    wordAndOne.keyBy(_._1)
      .map(new RichMapFunction[(String, Int), (String, Int)] {

        var counter: ValueState[Int] = _

        override def open(parameters: Configuration): Unit = {
          val ttlConfig: StateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(10)) // 时间是分不同的keyed的
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
          counter = getRuntimeContext.getState(new ValueStateDescriptor[Int]("wc", classOf[Int]))
        }

        override def map(value: (String, Int)): (String, Int) = {
          val count: Int = value._2
          var historyCount: Int = counter.value()
          if (historyCount == null) {
            historyCount = 0
          }
          historyCount += count
          counter.update(historyCount)
          (value._1, historyCount)
        }
      }).print()

    env.execute()
  }
}
