package com.ngt.state

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author ngt
 * @create 2021-02-09 7:52
 */
object RestartStrategyDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)
    // 未设置重启策略的时候程序出现异常就会退出，设置重启策略
    // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));

    // 10s一次Checkpointing，开启Checkpointing之后默认的重启策略就是无限重启
    // env.enableCheckpointing(10000);
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(30), Time.seconds(3)));
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
      .sum(1)
      .print()
    env.execute()
  }
}

/*
    https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/task_failure_recovery.html
                  无重启

    Fixed Delay Restart Strategy
    固定延时重启策略按照给定的次数尝试重启作业。 如果尝试超过了给定的最大次数，作业将最终失败。
    在连续的两次重启尝试之间，重启策略等待一段固定长度的时间。
    RestartStrategies.fixedDelayRestart(3, 5000) 最多重启3次，每5s一次

    Failure Rate Restart Strategy
    故障率重启策略在故障发生之后重启作业，但是当故障率（每个时间间隔发生故障的次数）超过设定的限制时，
    作业会最终失败。 在连续的两次重启尝试之间，重启策略等待一段固定长度的时间。
    RestartStrategies.failureRateRestart(3, Time.seconds(30), Time.seconds(3)) 30秒内错误达到 3 次就终止，超过30秒之后又重新开始计数
    30s 内最多重启3次，每次间隔3秒，超过30秒就可以从头开始

    No Restart Strategy
    作业直接失败，不尝试重启。
    RestartStrategies.noRestart()
    全局策略 fink-conf.yaml

    enableCheckpointing(10000);
    10s一次Checkpointing，开启Checkpointing之后默认的重启策略就是无限重启
 */