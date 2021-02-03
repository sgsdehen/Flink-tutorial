package com.ngt.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

/**
 * @author ngt
 * @create 2021-02-03 20:15
 */
object CountWindowAllDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val nums: DataStream[Int] = lines.map(_.toInt)
    // 将输入的数据每 4 个一组，然后2个滚动一次求和
    val window: AllWindowedStream[Int, GlobalWindow] = nums.countWindowAll(4, 2)
    window.sum(0).print()
    env.execute()
  }
}

/*
3
4  →  7
5
2  →  14
8
9  →  24
注意当输入两个数据的时候就会输出结果
 */