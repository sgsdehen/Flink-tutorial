package com.ngt.sink

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala._

import java.util.concurrent.TimeUnit

/**
 * @author ngt
 * @create 2021-01-26 23:53
 */
object StreamingFileSinkDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "ngt")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)
    env.enableCheckpointing(5000);

    val rollingPolicy: DefaultRollingPolicy[String, String] = DefaultRollingPolicy.builder()
      .withRolloverInterval(TimeUnit.SECONDS.toMillis(30)) // 30秒滚动生成一个文件
      .withInactivityInterval(TimeUnit.SECONDS.toMillis(5)) // 最近 5 秒没有收到新的记录
      .withMaxPartSize(1240L * 1024L * 1000L) //当文件达到100M滚动生成一个文件
      .build()

    val sink: StreamingFileSink[String] = StreamingFileSink.forRowFormat(
      new Path("data/out"), //指的文件存储目录
      new SimpleStringEncoder[String]("UTF-8")) //指的文件的编码
      .withRollingPolicy(rollingPolicy) //传入文件滚动生成策略
      .build()

    lines.addSink(sink)
    env.execute()
  }
}
