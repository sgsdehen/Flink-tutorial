package com.ngt.demo.orderpaydetect

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

import java.sql.Timestamp
import java.time.Duration
import java.util

/**
 * @author ngt on 2021-05-23 17:46
 * @version 1.0
 */
object OrderTimeoutWithCEP {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val inputStream: DataStream[String] = env.readTextFile("data/OrderLog.csv")

    val orderEventStream: DataStream[OrderEvent] = inputStream
      .map(data => {
        val arr: Array[String] = data.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3))
        .withTimestampAssigner(new SerializableTimestampAssigner[OrderEvent] {
          override def extractTimestamp(element: OrderEvent, recordTimestamp: Long): Long = element.timestamp * 1000L
        }))
      .keyBy(_.orderId)

    // 1. 定义一个pattern
    val orderPayPattern: Pattern[OrderEvent, OrderEvent] = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 2. 将pattern应用到数据流上，进行模式检测
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream, orderPayPattern)

    // 3. 定义侧输出流标签，用于处理超时事件
    val orderTimeoutOutputTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("orderTimeout")

    // 4. 调用select方法，提取并处理匹配的成功支付事件以及超时事件
    val resultStream: DataStream[OrderResult] =
      patternStream.select(orderTimeoutOutputTag, new OrderTimeoutSelect, new OrderPaySelect)

    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")
    env.execute("order timeout job")
  }
}


class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeoutOrderId: Long = map.get("create").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout " + ":" + new Timestamp(l))
  }
}

class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId: Long = map.get("pay").iterator().next().orderId
    OrderResult(payedOrderId, "payed successfully")
  }
}