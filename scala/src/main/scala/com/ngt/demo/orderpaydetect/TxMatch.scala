package com.ngt.demo.orderpaydetect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.net.URL

/**
 * Created on 2021-05-23 22:04.
 *
 * @author ngt
 */
object TxMatch {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1. 读取订单事件数据
    val resource1: URL = getClass.getResource("/OrderLog.csv")
    val orderEventStream: KeyedStream[OrderEvent, String] = env.readTextFile(resource1.getPath)
      .map(data => {
        val arr: Array[String] = data.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.eventType == "pay")
      .keyBy(_.txId)


    // 2. 读取到账事件数据
    val resource2: URL = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream: KeyedStream[ReceiptEvent, String] = env.readTextFile(resource2.getPath)
      .map(data => {
        val arr: Array[String] = data.split(",")
        ReceiptEvent(arr(0), arr(1), arr(2).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.txId)

    // 3. 合并两条流，进行处理
    //合并之前使用keyby那么就是用 CoProcessFunction， 如果是合并之后再使用keyby那么必须要使用 KeyCoProcessFunction
    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream.connect(receiptEventStream)
      .process(new TxPayMatchResult)

    resultStream.print("matched")
    resultStream.getSideOutput(new OutputTag[OrderEvent]("unmatched-pay")).print("unmatched pays")
    resultStream.getSideOutput(new OutputTag[ReceiptEvent]("unmatched-receipt")).print("unmatched receipts")

    env.execute("tx match job")
  }
}

case class ReceiptEvent(txId: String, payChannel: String, timestamp: Long)

class TxPayMatchResult extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
  private lazy val payEventState: ValueState[OrderEvent] =
    getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay", classOf[OrderEvent]))

  private lazy val receiptEventState: ValueState[ReceiptEvent] =
    getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt", classOf[ReceiptEvent]))

  // 侧输出流标签
  val unmatchedPayEventOutputTag: OutputTag[OrderEvent] =
    new OutputTag[OrderEvent]("unmatched-pay")

  val unmatchedReceiptEventOutputTag: OutputTag[ReceiptEvent] =
    new OutputTag[ReceiptEvent]("unmatched-receipt")

  // 处理支付事件
  override def processElement1(value: OrderEvent,
                               ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                               out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val receipt: ReceiptEvent = receiptEventState.value()
    if (receipt != null) {
      out.collect((value, receipt))
      receiptEventState.clear()
      payEventState.clear()
    } else {
      ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L + 5000L)
      payEventState.update(value)
    }
  }

  // 处理到账事件
  override def processElement2(value: ReceiptEvent,
                               ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                               out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val pay: OrderEvent = payEventState.value()
    if (pay != null) {
      out.collect((pay, value))
      receiptEventState.clear()
      payEventState.clear()
    } else {
      // 如果还没来，注册定时器开始等待3秒
      ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L + 3000L)
      // 更新状态
      receiptEventState.update(value)
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext,
                       out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    if (payEventState.value() != null) ctx.output(unmatchedPayEventOutputTag, payEventState.value())
    if (receiptEventState.value() != null) ctx.output(unmatchedReceiptEventOutputTag, receiptEventState.value())
    receiptEventState.clear()
    payEventState.clear()
  }
}
