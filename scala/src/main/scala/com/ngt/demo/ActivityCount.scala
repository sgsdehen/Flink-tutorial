package com.ngt.demo

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * @author ngt
 * @create 2021-02-10 22:44
 */
object ActivityCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val dataStream: DataStream[(String, String, String)] = lines.map(data => {
      val strings: Array[String] = data.split(",")
      (strings(0), strings(1), strings(2))
    })

    val keyBy: KeyedStream[(String, String, String), (String, String)] = dataStream.keyBy(data => (data._2, data._3))

    keyBy.process(new KeyedProcessFunction[(String, String), (String, String, String), (String, String, Long, Long)] {

      private var countState: ValueState[Long] = _
      private var userState: ValueState[mutable.HashSet[String]] = _

      override def open(parameters: Configuration): Unit = {
        countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("state1", classOf[Long]))
        userState = getRuntimeContext.getState(new ValueStateDescriptor[mutable.HashSet[String]]("state1", TypeInformation.of(classOf[mutable.HashSet[String]])))
      }

      override def processElement(value: (String, String, String), ctx: KeyedProcessFunction[(String, String), (String, String, String), (String, String, Long, Long)]#Context, out: Collector[(String, String, Long, Long)]): Unit = {
        var historyCount: Long = countState.value()
        if (historyCount == null) {
          historyCount = 0L
        }
        countState.update(historyCount)
        var user: mutable.HashSet[String] = userState.value()

        if (user == null) {
          user = new mutable.HashSet[String]()
        }
        user.add(value._1)
        userState.update(user)
        out.collect((value._2, value._3, historyCount, user.size.toLong))
      }
    }).print()
    env.execute()
  }
}
