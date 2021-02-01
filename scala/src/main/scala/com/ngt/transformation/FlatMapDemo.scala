package com.ngt.transformation

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.operators.{AbstractStreamOperator, OneInputStreamOperator}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.util.Collector

/**
 * @author ngt
 * @create 2021-02-02 0:48
 */
object FlatMapDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)


    lines.flatMap(_.split(" "))
      .filter(data => !"error".equals(data))
      .print()

    lines.flatMap(new FlatMapFunction[String,String] {
      override def flatMap(value: String, out: Collector[String]): Unit = {
        val strings: Array[String] = value.split(" ")
        for (elem <- strings) {
          if(!"error".equals(elem)){
            out.collect(elem)
          }
        }
      }
    }).print()

    // 同 map 可以调用 transform，更底层的实现方式
    lines.transform("myFlatMa", new MyFlatMap).print()
    env.execute()

  }

  case class MyFlatMap() extends  AbstractStreamOperator[String] with OneInputStreamOperator[String,String]{
    override def processElement(element: StreamRecord[String]): Unit = {
      val value: String = element.getValue
      val strings: Array[String] = value.split(" ")
      for (elem <- strings) {
        if(!"error".equals(elem)){
          output.collect(element.replace(elem))
        }
      }
    }
  }
}
