package com.ngt.sink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @author ngt
 * @create 2021-01-26 21:40
 */
object RedisSinkDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val words: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val summed: DataStream[(String, Int)] = words
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)

    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("192.168.31.8").setPort(6379).build()
    summed.addSink(new RedisSink[(String, Int)](config, new MyRidesSink))
    env.execute()
  }

  case class MyRidesSink() extends RedisMapper[(String, Int)] {
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET, "wordconut")
    }

    override def getKeyFromData(data: (String, Int)): String = data._1

    override def getValueFromData(data: (String, Int)): String = data._2.toString
  }
}
