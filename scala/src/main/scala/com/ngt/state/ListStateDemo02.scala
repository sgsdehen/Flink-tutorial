package com.ngt.state

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author ngt
 * @create 2021-02-08 21:43
 */
object ListStateDemo02 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    // 启用 Checkpointing
    env.enableCheckpointing(5000)
    // 未设置重启策略的时候程序出现异常就会退出，设置重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)))

    val tpDataStream: DataStream[(String, String)] = lines.map(data => {
      val words: Array[String] = data.split(",")
      (words(0), words(1))
    })

    tpDataStream.keyBy(_._1)
      .process(new KeyedProcessFunction[String, (String, String), (String, ListBuffer[String])] {
        // scala 中的状态可以使用 lazy，也可以使用 open，使用open的时候和 Java 写法一样

        var listState: ValueState[ListBuffer[String]] = _

        override def open(parameters: Configuration): Unit = {
          // 注意非基本类型要使用  TypeInformation.of(classOf[ListBuffer[String]])
          val stateDescriptor: ValueStateDescriptor[ListBuffer[String]] = new ValueStateDescriptor[ListBuffer[String]]("wc", TypeInformation.of(classOf[ListBuffer[String]]))
          listState = getRuntimeContext.getState(stateDescriptor)
        }

        override def processElement(value: (String, String),
                                    ctx: KeyedProcessFunction[String, (String, String), (String, ListBuffer[String])]#Context, out: Collector[(String, ListBuffer[String])]): Unit = {
          val action: String = value._2
          var lst: ListBuffer[String] = listState.value()
          if (lst == null) {
            lst = new ListBuffer[String]
          }
          lst += action
          listState.update(lst)
          out.collect((value._1, lst))
        }
      })
      .print()

    env.execute()
  }
}

/*
user1,A
user1,B
user2,D
user2,C
ueer1,C

1> (user2,ListBuffer(C))
6> (user1,ListBuffer(A))
1> (user2,ListBuffer(C, D))
6> (user1,ListBuffer(A, B))
7> (ueer1,ListBuffer(C))
 */