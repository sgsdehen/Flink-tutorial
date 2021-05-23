package com.ngt.demo.orderpaydetect

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * Created on 2021-05-23 17:09
 *
 * @author ngt
 *         使用传统方法查找超时的订单：提交之后超时未支付的订单
 */
object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val inputStream: DataStream[String] = env.readTextFile("data/LoginLog.csv")

    val orderEventStream: DataStream[OrderEvent] = inputStream
      .map(data => {
        val arr: Array[String] = data.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      //      .assignAscendingTimestamps(_.timestamp*1000L)
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3))
        .withTimestampAssigner(new SerializableTimestampAssigner[OrderEvent] {
          override def extractTimestamp(element: OrderEvent, recordTimestamp: Long): Long = element.timestamp * 1000L
        }))

    val orderResultStream: DataStream[OrderResult] = orderEventStream
      .keyBy(_.orderId)
      .process(new OrderPayMatchResult())

    orderResultStream.getSideOutput(new OutputTag[OrderResult]("timeout")).print("timeout")
    orderResultStream.print("payed")
    env.execute("order timeout without cep")
  }
}

case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)

case class OrderResult(orderId: Long, resultMsg: String)

class OrderPayMatchResult extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

  private lazy val isCreatedState: ValueState[Boolean] =
    getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-created", classOf[Boolean]))

  private lazy val isPayedState: ValueState[Boolean] =
    getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-payed", classOf[Boolean]))

  private lazy val timerTsState: ValueState[Long] =
    getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  // 定义侧输出流
  val orderTimeoutOutputTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("timeout")

  override def processElement(value: OrderEvent,
                              ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context,
                              out: Collector[OrderResult]): Unit = {
    // 先拿到当前状态
    val isPayed: Boolean = isPayedState.value()
    val isCreated: Boolean = isCreatedState.value()
    val timerTs: Long = timerTsState.value()

    // 1. 来的是create，要继续判断是否pay过
    if (value.eventType == "create") {
      if (isPayed) {
        out.collect(OrderResult(value.orderId, "payed successfully"))
        // 已经处理完毕，清空状态和定时器
        isCreatedState.clear()
        isPayedState.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      } else {
        val ts: Long = value.timestamp * 1000L + 15 * 60 * 1000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
        isCreatedState.update(true)
      }
    } else if (value.eventType == "pay") {
      // 2. 如果来的是pay，要判断是否有下单事件来过
      if (isCreated) {
        // 2.1 已经有过下单事件，要继续判断支付的时间戳是否超过15分钟
        if (value.timestamp * 1000L < timerTs) {
          // 2.1.1 没有超时，正常输出
          out.collect(OrderResult(value.orderId, "payed successfully"))
        } else {
          // 2.1.2 已经超时，输出超时
          ctx.output(orderTimeoutOutputTag, OrderResult(value.orderId, "payed but already timeout"))
        }
        // 只要输出结果，当前order处理已经结束，清空状态和定时器
        isCreatedState.clear()
        isPayedState.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      } else {
        // 2.2 如果create没来，注册定时器，等到pay的时间就可以
        ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L)
        // 更新状态
        timerTsState.update(value.timestamp * 1000L)
        isPayedState.update(true)
      }
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext,
                       out: Collector[OrderResult]): Unit = {
    // 1. pay来了，没等到create
    if (isPayedState.value()) {
      ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "payed but not found create log"))
    } else {
      // 2. create来了，没有pay
      ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
    }
    // 清空状态
    isCreatedState.clear()
    isPayedState.clear()
    timerTsState.clear()
  }
}