package com.ngt.demo.orderpaydetect

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.{KeyedStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @author ngt on 2021-05-24 9:25
 * @version 1.0
 */
object TxMatchWithJoin {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1. 读取订单事件数据
    val orderEventStream: KeyedStream[OrderEvent, String] = env.readTextFile("data/OrderLog.csv")
      .map(data => {
        val arr: Array[String] = data.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.eventType == "pay")
      .keyBy(_.txId)


    // 2. 读取到账事件数据
    val receiptEventStream: KeyedStream[ReceiptEvent, String] = env.readTextFile("data/ReceiptLog.csv")
      .map(data => {
        val arr: Array[String] = data.split(",")
        ReceiptEvent(arr(0), arr(1), arr(2).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.txId)

    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream.intervalJoin(receiptEventStream)
      .between(Time.seconds(-5), Time.seconds(10))
      .process(new TxMatchWithJoinResult)

    resultStream.print("tx match with join job")

    env.execute()
  }
}

class TxMatchWithJoinResult extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
  override def processElement(left: OrderEvent,
                              right: ReceiptEvent,
                              ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                              out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect((left, right))
  }
}
