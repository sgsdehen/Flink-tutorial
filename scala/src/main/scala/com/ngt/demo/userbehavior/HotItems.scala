package com.ngt.demo.userbehavior

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.util
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer

/**
 * @author ngt
 * @create 2021-05-21 17:11
  每隔5分钟输出最近一小时内点击量最多的前N个商品。将这个需求进行分解我们大概要做这么几件事情：
    抽取出业务时间戳，告诉Flink框架基于业务时间做窗口
    过滤出点击行为数据
    按一小时的窗口大小，每5分钟统计一次，做滑动窗口聚合（Sliding Window）
    按每个窗口聚合，输出每个窗口中点击量前N名的商品
 */
object HotItems {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val lines: DataStream[String] = env.readTextFile("data/UserBehavior.csv")

    val dataStream: DataStream[UserBehavior] = lines.map(line => {
      val arr: Array[String] = line.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    })
      //      .assignAscendingTimestamps(_.timestamp * 1000L)
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forMonotonousTimestamps[UserBehavior]()
        .withTimestampAssigner(new SerializableTimestampAssigner[UserBehavior] {
          override def extractTimestamp(element: UserBehavior, recordTimestamp: Long): Long = element.timestamp * 1000L
        }))

    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .window(SlidingEventTimeWindows.of(Time.minutes(60), Time.minutes(5)))
      .aggregate(new CountAgg, new ItemViewWindowResult)

    //    aggStream.print()
    val resultStream: DataStream[String] = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(5))

    resultStream.print()
    env.execute()

  }
}

// 定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

/*
使用 aggregate(new CountAgg(), new ItemViewWindowResult()) 做增量的聚合操作，
它能使用AggregateFunction提前聚合掉数据，减少state的存储压力
第二个参数WindowFunction主要是为了得到窗口信息，为后面分组提供依据
最终按照(主键商品ID，窗口，点击量)封装成了ItemViewCount进行输出。
*/
class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class ItemViewWindowResult extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long,
                     window: TimeWindow,
                     input: Iterable[Long],
                     out: Collector[ItemViewCount]): Unit = {
    val count: Long = input.iterator.next()
    out.collect(ItemViewCount(key, window.getEnd, count))
  }
}


class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  private lazy val itemViewCountListState: ListState[ItemViewCount] =
    getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewCount-list", classOf[ItemViewCount]))

  override def processElement(value: ItemViewCount,
                              ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    itemViewCountListState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    val allItemViewCounts: ListBuffer[ItemViewCount] = ListBuffer()
    val iter: util.Iterator[ItemViewCount] = itemViewCountListState.get().iterator()
    while (iter.hasNext) {
      allItemViewCounts += iter.next()
    }
    itemViewCountListState.clear()

    val sortedItemViewCounts: ListBuffer[ItemViewCount] = allItemViewCounts.sortBy(-_.count).take(topSize)

    val result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    // 使用 Timestamp - 1 就是当前窗口的时间
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedItemViewCounts.indices) {
      val currentItemViewCount: ItemViewCount = sortedItemViewCounts(i)
      result.append("NO").append(i + 1).append(": \t")
        .append("商品ID = ").append(currentItemViewCount.itemId).append("\t")
        .append("热门度 = ").append(currentItemViewCount.count).append("\n")
    }
    result.append("==================================\n")
    TimeUnit.MILLISECONDS.sleep(100L)
    out.collect(result.toString())
  }

}
