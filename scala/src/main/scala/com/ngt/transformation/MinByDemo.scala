package com.ngt.transformation

import org.apache.flink.streaming.api.scala._

/**
 * @author ngt
 * @create 2021-02-02 2:09
 *         min 和 minBy的区别
 */
object MinByDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    // 输入：省份,城市,金额
    val provinceCityAndMoney: DataStream[(String, String, Double)] = lines.map(data => {
      val words: Array[String] = data.split(",")
      (words(0), words(1), words(2).toDouble)
    })

    /*
       统一输入
A,b,3000
A,d,2000
A,f,2000
B,b,1000
    */
//    provinceCityAndMoney.keyBy(_._1).min(2).print()
    /*
    7> (A,f,2000.0)
    7> (A,f,2000.0)
    7> (A,f,2000.0)
    2> (B,b,1000.0)
     */

    provinceCityAndMoney.keyBy(_._1).minBy(2).print()
    /*
    7> (A,b,3000.0)
    7> (A,d,2000.0)
    7> (A,d,2000.0)
    2> (B,b,1000.0)
     */

//    provinceCityAndMoney.keyBy(_._1).minBy(2).print()

    env.execute()

  }
}
