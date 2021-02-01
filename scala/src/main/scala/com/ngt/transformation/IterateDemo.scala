package com.ngt.transformation

import org.apache.flink.streaming.api.datastream.{IterativeStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.scala._

/**
 * @author ngt
 * @create 2021-02-02 2:31
 * 出现了和 JDBCSink 同样的问题
 *
 * class org.apache.flink.streaming.api.scala.DataStream cannot be cast to
 * class org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
 * (org.apache.flink.streaming.api.scala.DataStream and org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
 * are in unnamed module of loader 'app')
 */
object IterateDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 输入整数
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val numbers: DataStream[Long] = lines.map(_.toLong)
    val iteration: IterativeStream[Long] = numbers.asInstanceOf[SingleOutputStreamOperator[Long]].iterate()

    // 迭代操作
    val iterationBody: SingleOutputStreamOperator[Long] = iteration.map(value => {
      println("iterate input =>" + value)
      value - 2
    })

    // 继续迭代的条件
    val feedback: SingleOutputStreamOperator[Long] = iterationBody.filter(_ > 0)

    // 传入迭代的条件
    iteration.closeWith(feedback)

    // 退出迭代的条件

    iterationBody.filter(_ <= 0).print("output value")
    env.execute()


  }
}
