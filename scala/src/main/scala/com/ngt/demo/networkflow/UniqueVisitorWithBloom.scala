package com.ngt.demo.networkflow

import com.ngt.demo.userbehavior.UserBehavior
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Created on 2021-05-22 20:48.
 *
 * @author ngt
 */
object UniqueVisitorWithBloom {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val lines: DataStream[String] = env.readTextFile("data/UserBehavior.csv")

    val dataStream: DataStream[UserBehavior] = lines.map(line => {
      val arr: Array[String] = line.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val uvStream: DataStream[UvCount] = dataStream
      .filter(_.behavior == "pv")
      .map(data => ("uv", data.userId))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.minutes(60L)))
      .trigger(new MyTrigger)
      .process(new UvCountWithBloom)

    uvStream.print()

    env.execute()
  }
}


// 自定义触发器

class MyTrigger extends Trigger[(String, Long), TimeWindow] {

  // 添加元素的时候调用 UvCountWithBloom 中的 process
  override def onElement(element: (String, Long),
                         timestamp: Long,
                         window: TimeWindow,
                         ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(time: Long,
                                window: TimeWindow,
                                ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onEventTime(time: Long,
                           window: TimeWindow,
                           ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow,
                     ctx: Trigger.TriggerContext): Unit = ???
}

class Bloom(size: Long) extends Serializable {
  private val cap = size

  // hash函数
  def hash(value: String, seed: Int): Long = {
    var result: Int = 0
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }
    // 返回hash值，要映射到cap范围内
    (cap - 1) & result
  }
}

class UvCountWithBloom extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
  private val jedis: Jedis = new Jedis("192.168.31.8", 6379)
  private val bloomFilter: Bloom = new Bloom(1 << 19)

  // 本来是收集齐所有数据、窗口触发计算的时候才会调用；现在每来一条数据都调用一次
  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, Long)],
                       out: Collector[UvCount]): Unit = {
    val currentKey: String = context.window.getEnd.toString
    val uvCountMap: String = "uvcount"
    var count: Long = 0L
    // 从redis中取出当前窗口的uv count值
    if (jedis.hget(uvCountMap, currentKey) != null) {
      count = jedis.hget(uvCountMap, currentKey).toLong
    }

    // 去重：判断当前userId的hash值对应的位图位置，是否为0
    val userId: String = elements.last._2.toString
    // 计算hash值，就对应着位图中的偏移量
    val offset = bloomFilter.hash(userId, 61)
    // 用redis的位操作命令，取bitmap中对应位的值
    val isExist = jedis.getbit(currentKey, offset)

    if (!isExist) {
      // 如果不存在，那么位图对应位置置1，并且将count值加1
      jedis.setbit(currentKey, offset, true)
      jedis.hset(uvCountMap, currentKey, (count + 1).toString)
    }

  }
}