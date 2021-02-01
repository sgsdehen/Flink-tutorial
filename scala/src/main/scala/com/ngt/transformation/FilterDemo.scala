package com.ngt.transformation

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.operators.{AbstractStreamOperator, OneInputStreamOperator}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord

/**
 * @author ngt
 * @create 2021-02-02 0:27
 */
object FilterDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val nums: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 0)

    nums.filter(data => (data & 1) == 0).print()

    nums.filter(new FilterFunction[Int] {
      override def filter(value: Int): Boolean = {
        (value & 1) == 0
      }
    }).print()

    // 同 map 可以调用 transform，更底层的实现方式
    nums.transform("myfilter", new MyStreamFilter).print()
    env.execute()

  }

  // 更底层的实现方式
  case class MyStreamFilter() extends  AbstractStreamOperator[Int] with OneInputStreamOperator[Int, Int]{
    override def processElement(element: StreamRecord[Int]): Unit = {
      val value: Int = element.getValue()
      if((value & 1) == 0){
        output.collect(element.replace(value))
      }
    }
  }
}
