package com.ngt.demo.loginfail

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

import java.time.Duration
import java.util
import scala.collection.mutable.ListBuffer

/**
 * Created on 2021-05-22 22:27.
 *
 * @author ngt
 */
object LoginFail {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(1000L)
    val inputStream: DataStream[String] = env.readTextFile("data/LoginLog.csv")

    val loginEventStream: DataStream[LoginEvent] = inputStream
      .map(data => {
        val arr: Array[String] = data.split(",")
        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3))
        .withTimestampAssigner(new SerializableTimestampAssigner[LoginEvent] {
          override def extractTimestamp(element: LoginEvent, recordTimestamp: Long): Long = element.timestamp * 1000L
        }))

    // 进行判断和检测，如果2秒之内连续登录失败，输出报警信息
    val loginFailWarningStream: DataStream[LoginFailWarning] = loginEventStream
      .keyBy(_.userId)
      .process(new LoginFailWarningResult(2))

    loginFailWarningStream.print()
    env.execute("login fail detect job")
  }

}

// 输入的登录事件样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, timestamp: Long)

// 输出报警信息样例类
case class LoginFailWarning(userId: Long, firstFailTime: Long, lastFailTime: Long, waringMsg: String)


// 注意使用该策略只能将 两秒内 一直失败且失败次数大于阈值的数据找出来
// 对于两秒内 失败次数大于阈值，但存在登录成功的情况 不能查找出来
class LoginFailWarningResult(failTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {
  //定义状态，保存当前所有的登录失败事件，保存定时器的时间戳
  lazy val loginFailListState: ListState[LoginEvent] =
    getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list", classOf[LoginEvent]))

  lazy val timerTsState: ValueState[Long] =
    getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  override def processElement(value: LoginEvent,
                              ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context,
                              out: Collector[LoginFailWarning]): Unit = {
    if (value.eventType == "fail") {
      loginFailListState.add(value)
      if (timerTsState.value() == 0) {
        val ts: Long = value.timestamp * 1000L + 2000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
      } else {
        // 如果是成功，那么直接清空状态和定时器，重新开始
        ctx.timerService().deleteEventTimeTimer(timerTsState.value())
        loginFailListState.clear()
        timerTsState.clear()
      }
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#OnTimerContext,
                       out: Collector[LoginFailWarning]): Unit = {
    val allLoginFailList: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]
    val iter: util.Iterator[LoginEvent] = loginFailListState.get().iterator()
    while (iter.hasNext) {
      allLoginFailList += iter.next()
    }

    if (allLoginFailList.length > failTimes) {
      out.collect(
        LoginFailWarning(
          allLoginFailList.head.userId,
          allLoginFailList.head.timestamp,
          allLoginFailList.last.timestamp,
          "login fail in 2s for " + allLoginFailList.length + " times."
        ))
    }

    loginFailListState.clear()
    timerTsState.clear()
  }
}