package com.ngt.demo.networkflow

import com.ngt.demo.hotitems.UserBehavior
import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * @author ngt
 * @date 2021-05-21 21:47
 * 实现一个网站总浏览量的统计。我们可以设置滚动时间窗口，实时统计每小时内的网站PV
 */
object PageView {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream: DataStream[String] = env.readTextFile("data/UserBehavior.csv")

    val dataStream: DataStream[UserBehavior] = inputStream.map(data => {
      val arr: Array[String] = data.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    }
    )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val pvStream: DataStream[PvCount] = dataStream
      .filter(_.behavior == "pv")
      .map(_ => ("pv", 1L))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.minutes(60)))
      .aggregate(new PvCountAgg(), new PvCountWindowResult())

    val totalPvStream: DataStream[PvCount] = pvStream
      .keyBy(_.windowEnd)
      .process(new TotalPvCountResult())


    totalPvStream.print("totalPvStream")

    val pvStream1: DataStream[PvCount] = dataStream
      .filter(_.behavior == "pv")
      .map(new MyMapper())
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.minutes(60)))
      .aggregate(new PvCountAgg(), new PvCountWindowResult())

    val totalPvStream1: DataStream[PvCount] = pvStream1
      .keyBy(_.windowEnd)
      .process(new TotalPvCountResult())

    totalPvStream1.print("totalPvStream1")
    env.execute()
  }
}

// 定义输出pv统计的样例类
case class PvCount(windowEnd: Long, count: Long)

class PvCountAgg extends AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class PvCountWindowResult extends WindowFunction[Long, PvCount, String, TimeWindow] {
  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[Long],
                     out: Collector[PvCount]): Unit = {
    out.collect(PvCount(window.getEnd, input.iterator.next()))
  }
}

class TotalPvCountResult() extends KeyedProcessFunction[Long, PvCount, PvCount] {
  lazy val totalPvCountResultState: ValueState[Long] =
    getRuntimeContext.getState(new ValueStateDescriptor[Long]("total-pv", classOf[Long]))

  override def processElement(value: PvCount, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#Context, out: Collector[PvCount]): Unit = {
    val currentTotalCount: Long = totalPvCountResultState.value()
    totalPvCountResultState.update(currentTotalCount + value.count)
    // 注册一个windowEnd+1ms后触发的定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
    val totalPvCount: Long = totalPvCountResultState.value()
    out.collect(PvCount(ctx.getCurrentKey, totalPvCount))
    totalPvCountResultState.clear()
  }
}

// 实现数据均衡
class MyMapper() extends MapFunction[UserBehavior, (String, Long)]{
  override def map(value: UserBehavior): (String, Long) = {
    ( Random.nextString(10), 1L )
  }
}