package com.ngt.sink

import org.apache.flink.streaming.api.scala._

import java.io.File

/**
 * @author ngt
 * @create 2021-01-26 19:40
 */
object WriteAsTextSinkDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val localhost: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    env.setParallelism(1)
    /*
      已经过期，因为不能保证，建议使用 StreamingFileSink 替换
      注意并行度不为1的时候，会生成多个分片文件
    */
    val textPath = "data/2.txt"
    val textfile = new File(textPath)
    if (textfile.exists()) {
      textfile.delete()
    }

    localhost.writeAsText(textPath)
    env.execute()
  }
}
