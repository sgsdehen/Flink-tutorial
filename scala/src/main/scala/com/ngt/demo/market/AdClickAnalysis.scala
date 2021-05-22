package com.ngt.demo.market

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

/**
 * @author ngt
 * @create 2021-05-22 16:27
 */
/*
我们可以对一段时间内（比如一天内）的用户点击行为进行约束，
如果对同一个广告点击超过一定限额（比如 100 次），应该把该用户加入黑名单并报警，此后其点击行为不应该再统计
*/
object AdClickAnalysis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val inputStream: DataStream[String] = env.readTextFile("data/AdClickLog.csv")

    val adLogStream: DataStream[AdClickLog] = inputStream
      .map(data => {
        val arr = data.split(",")
        AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val filterBlackListUserStream: DataStream[AdClickLog] = adLogStream
      .keyBy(data => (data.userId, data.adId))
      .process(new FliterBlackListUserResult(100))

    val adCountResultStream: DataStream[AdClickCountByProvince] = filterBlackListUserStream
      .keyBy(_.province)
      .window(TumblingEventTimeWindows.of(Time.minutes(60), Time.seconds(5)))
      .aggregate(new AdCountAgg, new AdCountWindowResult)

    adCountResultStream.print()
    filterBlackListUserStream.getSideOutput(new OutputTag[BlackListUserWarning]("warning")).print("waring")
    env.execute("ad count statistics job")
  }
}

// 定义输入输出样例类
case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class AdClickCountByProvince(windowEnd: String, province: String, count: Long)

// 侧输出流黑名单报警信息样例类
case class BlackListUserWarning(userId: Long, adId: Long, msg: String)


class AdCountAgg() extends AggregateFunction[AdClickLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdCountWindowResult() extends WindowFunction[Long, AdClickCountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdClickCountByProvince]): Unit = {
    val end: String = new Timestamp(window.getEnd).toString
    out.collect(AdClickCountByProvince(end, key, input.head))
  }
}


class FliterBlackListUserResult(maxCount: Long) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {
  // 定义状态，保存用户对广告的点击量，每天0点定时清空状态的时间戳，标记当前用户是否已经进入黑名单
  lazy val countState: ValueState[Long] =
    getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))

  lazy val resetTimerTsState: ValueState[Long] =
    getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-ts", classOf[Long]))

  lazy val isBlackState: ValueState[Boolean] =
    getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-black", classOf[Boolean]))

  override def processElement(value: AdClickLog,
                              ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context,
                              out: Collector[AdClickLog]): Unit = {
    // 判断只要是第一个数据来了，直接注册0点的清空状态定时器
    val curCount: Long = countState.value()
    if (curCount == 0) {
      val ts: Long = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000) - 8 * 60 * 60 * 1000
      resetTimerTsState.update(ts)
      ctx.timerService().registerProcessingTimeTimer(ts)
    }

    if (curCount >= maxCount) {
      // 判断是否已经在黑名单里，没有的话才输出侧输出流
      if (!isBlackState.value()) {
        isBlackState.update(true)
        ctx.output(new OutputTag[BlackListUserWarning]("warning"), BlackListUserWarning(value.userId, value.adId, "Click ad over " + maxCount + " times today."))
      }
      return
    }
    countState.update(curCount + 1)
    out.collect(value)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext,
                       out: Collector[AdClickLog]): Unit = {
    if (timestamp == resetTimerTsState.value()) {
      isBlackState.clear()
      countState.clear()
    }
  }
}