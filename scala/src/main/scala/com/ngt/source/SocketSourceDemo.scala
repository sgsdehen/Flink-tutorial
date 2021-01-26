package com.ngt.source

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
 * @author ngt
 * @create 2021-01-26 19:23
 */
object SocketSourceDemo {
  def main1(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    println("envParallelism:" + env.getParallelism)

    println("SocketSourceParallelism:" + lines.parallelism)
    val words: DataStream[String] = lines.flatMap(_.split(" "))

    println("DataStreamParallelism:" + words.parallelism)

    words.print()
    env.execute("SourceParallelism")
    /*
    envParallelism:8
    SocketSourceParallelism:1
    DataStreamParallelism:8
     */
  }

  def main(args: Array[String]): Unit = {
    val configuration: Configuration = new Configuration()
    configuration.setInteger("rest.port", 8181)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)
    val localhost: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    localhost.print()
    env.execute()
  }
}
