package com.ngt.UserBehaviorAnalysis

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.time.Duration

/**
 * @author ngt
 * @create 2021-05-21 18:37
 */
object ApacheLogAnalysis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val lines: DataStream[String] = env.readTextFile("data/apache.log")


    val dataStream: DataStream[ApacheLogEvent] = lines.map(data => {
      val arr: Array[String] = data.split(" ")
      // 对事件时间进行转换，得到时间戳 已经是毫秒
      val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val ts: Long = simpleDateFormat.parse(arr(3)).getTime
      ApacheLogEvent(arr(0), arr(1), ts, arr(5), arr(6))
    })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1))
        .withTimestampAssigner(new SerializableTimestampAssigner[ApacheLogEvent] {
          override def extractTimestamp(element: ApacheLogEvent, recordTimestamp: Long): Long = element.timestamp
        })
      )

    val aggStream: DataStream[PageViewCount] = dataStream
      .filter(_.method == "GET")
      .keyBy(_.url)
      .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
      .allowedLateness(Time.minutes(1)) // 容忍一分钟的迟到数据
      .aggregate(new PageCountAgg(), new PageViewCountWindowResult())


    val resultStream: DataStream[String] = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNHotPages(3))

    aggStream.getSideOutput(new OutputTag[ApacheLogEvent]("late")).print("late") // 已经关闭的窗口的数据 输出到侧输出流
    resultStream.print()

    env.execute()
  }
}


// 定义输入数据样例类
case class ApacheLogEvent(ip: String, userId: String, timestamp: Long, method: String, url: String)

// 定义窗口聚合结果样例类
case class PageViewCount(url: String, windowEnd: Long, count: Long)


class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}


class PageViewCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {

  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[Long],
                     out: Collector[PageViewCount]): Unit = {
    out.collect(PageViewCount(key, window.getEnd, input.iterator.next()))
  }
}


class TopNHotPages(n: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {


  override def processElement(value: PageViewCount,
                              ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context,
                              out: Collector[String]): Unit = {

  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
  }


}