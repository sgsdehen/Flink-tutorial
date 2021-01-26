package com.ngt.sink

import org.apache.flink.streaming.api.scala._

import java.io.File

/**
 * @author ngt
 * @create 2021-01-26 20:03
 */
object WriteAsCsvSinkDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val words: DataStream[String] = env.fromElements("spark", "flink", "hadoop", "spark", "flink", "flink")

    val summed: DataStream[(String, Int)] = words
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)

    val csvPath = "data/2.csv";
    val csvfile: File = new File(csvPath);
    if (csvfile.exists()) {
      csvfile.delete();
    }
    summed.writeAsCsv(csvPath)
    env.execute()
  }
}
