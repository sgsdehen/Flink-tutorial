package com.ngt.transformation

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.operators.{AbstractStreamOperator, OneInputStreamOperator, StreamMap}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord

/**
 * @author ngt
 * @create 2021-02-01 23:51
 */
object MapDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    // 1.匿名内部类
    val upperCase: DataStream[String] = lines.map(new MapFunction[String, String] {
      override def map(value: String): String = {
        value.toUpperCase
      }
    })

    // 2. lambda
    val upperCaseLambda: DataStream[String] = lines.map(_.toUpperCase)

    // 3. 手动调用 transform，注意此处只需要两个参数而Java多一个类型参数
    val myMap: DataStream[String] = lines.transform("MyMap", new StreamMap(_.toUpperCase))

    /*
      4. extends AbstractStreamOperator<OUT> implements OneInputStreamOperator<IN.OUT>

    */
    val myStreamMap: DataStream[String] = lines.transform("MyMap", MyStreamMap())

    System.out.println("mapParallelism: " + upperCase.parallelism)

    upperCase.print
    upperCaseLambda.print
    myMap.print
    myStreamMap.print

    env.execute();
  }

  case class MyStreamMap() extends AbstractStreamOperator[String] with OneInputStreamOperator[String, String] {
    override def processElement(element: StreamRecord[String]): Unit = {
      val value: String = element.getValue
      val upperCase: String = value.toUpperCase
      element.replace(upperCase)
      output.collect(element)
    }
  }

}
