package com.ngt.state

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author ngt
 * @create 2021-02-08 22:46
 */
object MapStateDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)))
    env.enableCheckpointing(5000)

    val tpDataStream: DataStream[(String, String, Double)] = lines.map(data => {
      val words: Array[String] = data.split(",")
      (words(0), words(1), words(2).toDouble)
    })

    tpDataStream.keyBy(_._2)
      .process(new KeyedProcessFunction[String, (String, String, Double), (String, String, Double)] {

        var mapState: MapState[String, Double] = _

        override def open(parameters: Configuration): Unit = {
          val kvstate: MapStateDescriptor[String, Double] =
            new MapStateDescriptor[String, Double]("kvstate", classOf[String], classOf[Double])
          mapState = getRuntimeContext.getMapState(kvstate)
        }

        override def processElement(value: (String, String, Double), ctx: KeyedProcessFunction[String, (String, String, Double), (String, String, Double)]#Context, out: Collector[(String, String, Double)]): Unit = {
          val city: String = value._2
          val money: Double = value._3

          var historyMoney: Double = mapState.get(city)
          if (historyMoney == null) {
            historyMoney = 0.0
          }
          historyMoney += money
          mapState.put(city, historyMoney)
          out.collect((value._1, value._2, historyMoney))
        }
      })
      .print()

    env.execute()
  }
}

/*
辽宁省,大连市,3000
辽宁省,沈阳市,2300
山东省,青岛市,3400
辽宁省,大连市,2300
山东省,青岛市,1240

1> (辽宁省,大连市,3000.0)
1> (辽宁省,大连市,5300.0)
1> (辽宁省,沈阳市,2300.0)
7> (山东省,青岛市,3400.0)
7> (山东省,青岛市,4640.0)
 */