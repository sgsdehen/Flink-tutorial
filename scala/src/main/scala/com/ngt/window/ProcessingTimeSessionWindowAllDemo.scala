package com.ngt.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * @author ngt
 * @create 2021-02-03 22:10
 */
object ProcessingTimeSessionWindowAllDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    // Scala编译器中有一个快速的方法会导致windowwall(或window())无法工作，此处不能识别Scala 中 Int 类型，因此要使用 Integer
    // https://issues.apache.org/jira/browse/FLINK-18599
    val nums: DataStream[Integer] = lines.map(Integer.parseInt(_).asInstanceOf[Integer])

    nums.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
      .sum(1)
      .print()

    /*
      There is a quick in the Scala compiler which causes windowAll (or window()) to not work when the type of the
      DataStream is a primitive type (Int in your case). If you change it to another type, for example ("foo", 1)
      (a tuple type), the code will work when you also adapt the subsequent reduce().
     */
    // 变换成元组
    val value: DataStream[(String, Int)] = lines.map(data => ("1", data.toInt))
    value.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
      .sum(1)
      .map(_._2)
      .print()

    env.execute()
  }
}

