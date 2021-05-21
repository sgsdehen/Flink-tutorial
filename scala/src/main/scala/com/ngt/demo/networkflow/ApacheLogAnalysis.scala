package com.ngt.demo.networkflow

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Duration
import java.util
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer

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

  private lazy val pageViewCountMapState: MapState[String, Long] =
    getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageViewCount-map", classOf[String], classOf[Long]))


  // 此处也可以使用ListState直接保留PageViewCount类型但是其读写效率不如 MapState 高
  lazy val urlState: ListState[PageViewCount] =
    getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("urlState-state", Types.of[PageViewCount]))

  override def processElement(value: PageViewCount,
                              ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    pageViewCountMapState.put(value.url, value.count)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    // 另外注册一个定时器用于处理容忍的延迟，1分钟之后触发，这时窗口已经彻底关闭，不再有聚合结果输出，可以清空状态
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 60000L)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    if (ctx.getCurrentKey + 60000L == timestamp) {
      pageViewCountMapState.clear()
      return
    }
    val allPageViewCounts: ListBuffer[(String, Long)] = ListBuffer()

    val iter: util.Iterator[util.Map.Entry[String, Long]] = pageViewCountMapState.entries().iterator()
    while (iter.hasNext) {
      val entry: util.Map.Entry[String, Long] = iter.next()
      allPageViewCounts += ((entry.getKey, entry.getValue))
    }
    val sortedPageViewCounts: ListBuffer[(String, Long)] = allPageViewCounts.sortWith(_._2 > _._2).take(n)
    val result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- sortedPageViewCounts.indices) {
      val currentItemViewCount = sortedPageViewCounts(i)
      result.append("NO").append(i + 1).append(":\t")
        .append("页面URL = ").append(currentItemViewCount._1).append("\t")
        .append("热门度 = ").append(currentItemViewCount._2).append("\n")
    }
    result.append("\n==================================\n\n")
    TimeUnit.MILLISECONDS.sleep(100L)
    out.collect(result.toString())
  }


}