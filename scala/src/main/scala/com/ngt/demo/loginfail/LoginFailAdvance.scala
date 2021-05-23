package com.ngt.demo.loginfail

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

import java.time.Duration
import java.util

/**
 * Created on 2021-05-22 22:50.
 *
 * @author ngt
 */
object LoginFailAdvance {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(1000L)
    val inputStream: DataStream[String] = env.readTextFile("data/LoginLog.csv")

    val loginEventStream: DataStream[LoginEvent] = inputStream
      .map(data => {
        val arr = data.split(",")
        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3))
        .withTimestampAssigner(new SerializableTimestampAssigner[LoginEvent] {
          override def extractTimestamp(element: LoginEvent, recordTimestamp: Long): Long = element.timestamp * 1000L
        }))


    // 进行判断和检测，如果2秒之内连续登录失败，输出报警信息
    val loginFailWarningStream: DataStream[LoginFailWarning] = loginEventStream
      .keyBy(_.userId)
      .process(new LoginFailWaringAdvanceResult())

    loginFailWarningStream.print()
    env.execute("login fail detect job")
  }
}

class LoginFailWaringAdvanceResult extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {
  lazy val loginFailListState: ListState[LoginEvent] =
    getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list", classOf[LoginEvent]))

  override def processElement(value: LoginEvent,
                              ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context,
                              out: Collector[LoginFailWarning]): Unit = {
    // 首先判断事件类型
    if (value.eventType == "fail") {
      // 1. 如果是失败，进一步做判断
      val iter: util.Iterator[LoginEvent] = loginFailListState.get().iterator()
      if (iter.hasNext) {

        val firstFailEvent: LoginEvent = iter.next()
        // 如果在2秒之内，输出报警
        if (value.timestamp < firstFailEvent.timestamp + 2) {
          out.collect(LoginFailWarning(value.userId, firstFailEvent.timestamp, value.timestamp, "login fail 2 times in 2s"))
        }
        // 不管报不报警，当前都已处理完毕，将状态更新为最近依次登录失败的事件
        loginFailListState.clear()
        loginFailListState.add(value)
      } else {
        // 1.2 如果没有，直接把当前事件添加到ListState中
        loginFailListState.add(value)
      }
    } else {
      // 2. 如果是成功，直接清空状态
      loginFailListState.clear()
    }
  }
}