package com.ngt.state

import org.apache.flink.api.common.functions.{FlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.{Configuration, QueryableStateOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author ngt
 * @create 2021-02-09 8:22
 */
object QueryableStateDemo {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger("rest.port", 8181)
    conf.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    env.enableCheckpointing(5000);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(3)));
//    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val wordAndOne: DataStream[(String, Int)] = lines.flatMap(new FlatMapFunction[String, (String, Int)] {
      override def flatMap(value: String, out: Collector[(String, Int)]): Unit = {
        val words: Array[String] = value.split(" ")
        for (elem <- words) {
          out.collect((elem, 1))
        }
      }
    })

    wordAndOne.keyBy(_._1)
      .map(new MyQueryStateRichMapFunction)

  }

  case class MyQueryStateRichMapFunction() extends RichMapFunction[(String, Int), (String, Int)] {

    private var countState: ValueState[Int] = _


    override def open(parameters: Configuration): Unit = {
      val stateDescriptor: ValueStateDescriptor[Int] =
        new ValueStateDescriptor[Int]("wc-state", classOf[Int])

      stateDescriptor.setQueryable("my-query-name")
      countState = getRuntimeContext().getState(stateDescriptor)
    }

    override def map(value: (String, Int)): (String, Int) = {
      val count: Int = value._2
      var historyCount: Int = countState.value()
      if (historyCount == null) {
        historyCount = 0
      }
      historyCount += count
      countState.update(historyCount)
      (value._1, historyCount)
    }
  }

}
