package com.ngt.function

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author ngt
 * @create 2021-02-05 14:48
 */
object keyedProcessFunctionDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val tpDataStream: DataStream[(String, String, Double)] = lines.flatMap(new FlatMapFunction[String, (String, String, Double)]() {
      override def flatMap(value: String, out: Collector[(String, String, Double)]): Unit = {
        val strings: Array[String] = value.split(",")
        out.collect(strings(0), strings(1), strings(2).toDouble)
      }
    })

    tpDataStream.keyBy(_._1)
      .process(new MyKeyedProcessFunction)
      .print()

    env.execute()
  }

  class MyKeyedProcessFunction() extends KeyedProcessFunction[String, (String, String, Double), (String, String, Double)] {


    // scala 中的状态可以使用 lazy，也可以使用 open，使用open的时候和 Java 写法一样
    // lazy private val mapState: MapState[String, Double] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Double]("kvstate", classOf[String], classOf[Double]))

    private var mapState: MapState[String, Double] = _

    override def open(parameters: Configuration): Unit = {
      val kvstate: MapStateDescriptor[String, Double] = new MapStateDescriptor[String, Double]("kvstate", classOf[String], classOf[Double])
      mapState = getRuntimeContext.getMapState(kvstate)
    }

    override def processElement(value: (String, String, Double),
                                ctx: KeyedProcessFunction[String, (String, String, Double),
                                  (String, String, Double)]#Context, out: Collector[(String, String, Double)]): Unit = {
      val city: String = value._2
      val money: Double = value._3
      var historyMoney: Double = mapState.get(city)
      if (historyMoney == null) {
        historyMoney = 0.0;
      }
      historyMoney += money
      mapState.put(city, historyMoney)
      out.collect((value._1, value._2, historyMoney))
    }
  }

}

/*
辽宁省,大连市,3000
辽宁省,沈阳市,2300
山东省,青岛市,3400
山东省,青岛市,4572
辽宁省,大连市,4510
 */