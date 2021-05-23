package com.ngt.demo.loginfail

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration
import java.util

/**
 * Created on 2021-05-23 16:51
 *
 * @author ngt
 */
object LoginFailWithCep {
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

    // 1. 定义匹配的模式，要求是一个登录失败事件后，紧跟另一个登录失败事件
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
      .begin[LoginEvent]("firstFail").where(_.eventType == "fail")
      .next("secondFail").where(_.eventType == "fail")
      .next("thirdFail").where(_.eventType == "fail")
      .within(Time.seconds(5))

    // 还可以使用times() 定义3次匹配，默认情况下是使用宽松模式，使用consecutive()可以将其限定为严格模式
    val loginFailPattern1: Pattern[LoginEvent, LoginEvent] = Pattern
      .begin[LoginEvent]("fail").where(_.eventType == "fail")
      .times(3).consecutive()
      .within(Time.seconds(5))

    // 2. 将模式应用到数据流上，添加对于的标识，得到一个PatternStream
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)

    // 3. 检出符合模式的数据流，需要调用select
    val loginFailWarningStream: DataStream[LoginFailWarning] = patternStream.select(new LoginFailEventMatch())

    loginFailWarningStream.print()

    env.execute("login fail with cep job")
  }
}

class LoginFailEventMatch extends PatternSelectFunction[LoginEvent,LoginFailWarning]{
  override def select(map: util.Map[String, util.List[LoginEvent]]): LoginFailWarning = {
    val firstFailEvent: LoginEvent = map.get("firstFail").get(0)
    val thirdFailEvent: LoginEvent = map.get("thirdFail").iterator().next()
    LoginFailWarning(firstFailEvent.userId, firstFailEvent.timestamp, thirdFailEvent.timestamp, "login fail")
  }
}