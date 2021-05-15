package com.ngt.state

import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * @author ngt
 * @create 2021-02-09 6:54
 * 用户id，活动id，事件类型(1浏览，2参与)
 * user1,A,1
 * User1,A,1
 * User1,A,2
 * User2,A,1
 * User2,A,2
 * User3,A,2
 * User1,B,1
 * User1,B,2
 * User2,B,1
 * User3,A,1
 * User3,A,1
 * User3,B,1
 * User4,A,1
 * User4,A,1
 * 统计各个活动，事件的人数和次数
 * INSERT,1,浏览
 * INSERT,2,参与
 * INSERT,3,消费
 * UPDATE,3,退出
 * 使用 BroadcastState 将事实表和维度表进行连接操作
 * https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/stream/state/broadcast_state.html
 */
object BroadcastStateDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 接收事实输入 User4,A,1
    val act: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)
    val actStream: DataStream[(String, String, String)] = act.map(data => {
      val strings: Array[String] = data.split(",")
      (strings(0), strings(1), strings(2))
    })

    val keyByStream: KeyedStream[(String, String, String), (String, String)] = actStream.keyBy(data => (data._2, data._3))

    // 接收维度输入 INSERT,1,浏览
    val dic: DataStream[String] = env.socketTextStream("192.168.31.8", 9999)
    val dicStream: DataStream[(String, String, String)] = dic.map(data => {
      val strings: Array[String] = data.split(",")
      (strings(0), strings(1), strings(2))
    })


    val stateDescriptor: MapStateDescriptor[String, String] =
      new MapStateDescriptor[String, String]("dic-state", classOf[String], classOf[String])
    val broadcastStream: BroadcastStream[(String, String, String)] = dicStream.broadcast(stateDescriptor)

    keyByStream.connect(broadcastStream)
      .process(MyBroadcastProcessFunc(stateDescriptor))
      .print()

    env.execute()

  }

  case class MyBroadcastProcessFunc(stateDescriptor: MapStateDescriptor[String, String]) extends KeyedBroadcastProcessFunction[(String, String), (String, String, String), (String, String, String), (String, String, Long, Long)] {

    private val countState: ValueState[Long] =
      getRuntimeContext.getState(new ValueStateDescriptor[Long]("state1", classOf[Long]))
    private val userState: ValueState[mutable.HashSet[String]] =
      getRuntimeContext.getState(new ValueStateDescriptor[mutable.HashSet[String]]("state1", TypeInformation.of(classOf[mutable.HashSet[String]])))


    override def processElement(value: (String, String, String),
                                ctx: KeyedBroadcastProcessFunction[(String, String), (String, String, String), (String, String, String), (String, String, Long, Long)]#ReadOnlyContext,
                                out: Collector[(String, String, Long, Long)]): Unit = {
      val broadcastState: ReadOnlyBroadcastState[String, String] = ctx.getBroadcastState(stateDescriptor)
      var historyCount: Long = countState.value()
      if (historyCount == null) {
        historyCount = 0L
      }
      historyCount += 1
      countState.update(historyCount)
      // 计算用户数
      var user: mutable.HashSet[String] = userState.value()
      if (user == null) {
        user = new mutable.HashSet[String]
      }
      user.add(value._1)
      userState.update(user)

      val actId: String = value._3
      val actName: String = broadcastState.get(actId)
      out.collect((value._2, actName, historyCount, user.size.toLong))
    }

    override def processBroadcastElement(value: (String, String, String),
                                         ctx: KeyedBroadcastProcessFunction[(String, String), (String, String, String),  (String, String, String), (String, String, Long, Long)]#Context,
                                         out: Collector[(String, String, Long, Long)]): Unit = {
      val type1: String = value._1
      val actId: String = value._2
      val actName: String = value._3

      val broadcastState: BroadcastState[String, String] = ctx.getBroadcastState(stateDescriptor)
      if ("DELETE".equals(type1)) {
        broadcastState.remove(type1)
      } else {
        broadcastState.put(actId, actName)
      }
    }
  }

}

/*

INSERT,1,浏览
INSERT,2,参与
INSERT,3,消费
UPDATE,3,退出


user1,A,1
User1,A,1
User1,A,2
User2,A,1
User2,A,2
User3,A,2
User1,B,1
User1,B,2
User2,B,1
User3,A,1
User3,A,1
User3,B,1
User4,A,1
User4,A,1


 */