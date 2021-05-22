package com.ngt.demo.market

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.util.Random

/**
 * Created on 2021-05-22 18:04
 *
 * @author ngt
 */
object AppMarketByChannel {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setAutoWatermarkInterval(1000L)

    val dataStream: DataStream[MarketUserBehavior] = env.addSource(new SimulatedSource)
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forBoundedOutOfOrderness[MarketUserBehavior](Duration.ZERO)
        .withTimestampAssigner(new SerializableTimestampAssigner[MarketUserBehavior] {
          override def extractTimestamp(element: MarketUserBehavior, recordTimestamp: Long): Long = element.timestamp
        }))


    val resultStream: DataStream[MarketViewCount] = dataStream
      .filter(_.behavior != "uninstall")
      .keyBy(data => (data.channel, data.behavior)) // 同时使用渠道和行为作为分作的依据
      .window(SlidingEventTimeWindows.of(Time.hours(24), Time.seconds(5)))
      .process(new MarketCountByChannel)

    resultStream.print()
    env.execute("app market by channel job")
  }
}


// 定义输入数据样例类
case class MarketUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

// 定义输出数据样例类
case class MarketViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)


class SimulatedSource() extends RichSourceFunction[MarketUserBehavior] {
  var running: Boolean = true

  val behaviorSet: Seq[String] = Seq("view", "download", "install", "uninstall")
  val channelSet: Seq[String] = Seq("appstore", "weibo", "wechat", "tieba·")
  val rand: Random = Random

  override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
    val maxCount: Long = Long.MaxValue
    var count: Long = 0L
    while (running && count < maxCount) {
      val id: String = UUID.randomUUID().toString
      val behavior: String = behaviorSet(rand.nextInt(behaviorSet.size))
      val channel: String = channelSet(rand.nextInt(channelSet.size))
      val ts: Long = System.currentTimeMillis()

      ctx.collect(MarketUserBehavior(id, behavior, channel, ts))
      count += 1L
      TimeUnit.MILLISECONDS.sleep(50L)
    }

  }

  override def cancel(): Unit = {
    running = false
  }
}


class MarketCountByChannel extends ProcessWindowFunction[MarketUserBehavior, MarketViewCount, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketViewCount]): Unit = {
    val start: String = new Timestamp(context.window.getStart).toString
    val end: String = new Timestamp(context.window.getEnd).toString
    val channel: String = key._1
    val behavior: String = key._2
    val count: Int = elements.size
    out.collect(MarketViewCount(start, end, channel, behavior, count))
  }
}