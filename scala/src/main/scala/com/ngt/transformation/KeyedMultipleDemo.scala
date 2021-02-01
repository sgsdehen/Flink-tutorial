package com.ngt.transformation

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala._


/**
 * @author ngt
 * @create 2021-02-02 1:27
 */
object KeyedMultipleDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 输入：省份,城市,金额
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val provinceCityAndMoney: DataStream[(String, String, Double)] =
      lines.map(data => (data.split(",")(0), data.split(",")(1), data.split(",")(2).toDouble))

    // 1. 同时使用多个字段作为分组的依据
    provinceCityAndMoney.keyBy(0, 1).sum(2).print()

    // 2. 使用 KeySelector, 将两个字段相加
    provinceCityAndMoney.keyBy(new KeySelector[(String, String, Double), String] {
      override def getKey(value: (String, String, Double)): String = {
        value._1 + value._2
      }
    }).sum(2).print()

    // 3. 使用 KeySelector, 将两个字段构成一个元组

    provinceCityAndMoney.keyBy(new KeySelector[(String, String, Double), (String, String)] {
      override def getKey(value: (String, String, Double)): (String, String) = {
        (value._1, value._2)
      }
    }).sum(2).print()

    // 4. 使用 lambda 表达式，将两个字段相加
    provinceCityAndMoney.keyBy(data => data._1 + data._2).sum(2).print()


    // 5. 使用 lambda 表达式，将两个字段构成一个元组
    provinceCityAndMoney.keyBy(data => (data._1, data._2)).sum(2).print()
    env.execute()

  }

}
