package cn.bitnei

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * Created on 2021-06-01 20:40.
 *
 * @author ngt
 */
object BMWMileAvgCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val lines: DataStream[String] = env.readTextFile("data/baoma_label.csv")

    val cityList: List[String] = List(
      "上海市", "深圳市", "北京市", "成都市", "杭州市",
      "广州市", "重庆市", "南京市", "西安市", "长沙市",
      "苏州市", "台州市", "郑州市", "宁波市", "厦门市",
      "武汉市", "青岛市", "合肥市", "海口市", "无锡市")

    lines.filter(!_.contains("veh_model_name"))
      .map(line => {
        val arr: Array[String] = line.split(",")
        (arr(18), arr(1), arr(29).toDouble, arr(17).toInt,System.currentTimeMillis())
      }).filter(_._4 != 0)
      .assignAscendingTimestamps(data => data._5)
      .map(data => MileCount(data._1, data._2, data._3))
      .filter(data => cityList.contains(data.drive_city))
      .map(data => {
        if (data.mileAvgCount < 10) data.mileAvgCount = 0
        if (data.mileAvgCount >= 10 && data.mileAvgCount < 20) data.mileAvgCount = 10
        if (data.mileAvgCount >= 20 && data.mileAvgCount < 30) data.mileAvgCount = 20
        if (data.mileAvgCount >= 30 && data.mileAvgCount < 40) data.mileAvgCount = 30
        if (data.mileAvgCount >= 40 && data.mileAvgCount < 50) data.mileAvgCount = 40
        if (data.mileAvgCount >= 50 && data.mileAvgCount < 60) data.mileAvgCount = 50
        if (data.mileAvgCount >= 60 && data.mileAvgCount < 70) data.mileAvgCount = 60
        if (data.mileAvgCount >= 70) data.mileAvgCount = 70

        if (data.veh_common_name == "奔驰EQC350 4MATIC" || data.veh_common_name == "奔驰EQC400") {
          data.veh_common_name = "奔驰EQC"
        }

        if (data.veh_common_name == "宝马(BMW)IX3") {
          data.veh_common_name = "BMWiX3"
        }
        data
      })
      .map((_, 1))
      .keyBy(data => (data._1.drive_city, data._1.veh_common_name))
      .window(TumblingEventTimeWindows.of(Time.seconds(20)))
      .process(new MileCountWindow())
      .print()


    env.execute()

  }
}

case class MileCount(
                      drive_city: String,
                      var veh_common_name: String,
                      var mileAvgCount: Double
                    )


class MileCountWindow extends ProcessWindowFunction[(MileCount, Int), String, (String, String), TimeWindow] {
  override def process(key: (String, String),
                       context: Context,
                       elements: Iterable[(MileCount, Int)],
                       out: Collector[String]): Unit = {
    val list: ListBuffer[Int] = new ListBuffer[Int]
    for (i <- 1 to 8) {
      list += 0
    }
    val iterator: Iterator[(MileCount, Int)] = elements.iterator
    for (elem <- iterator) {
      list(elem._1.mileAvgCount.toInt / 10) += elem._2
    }
    val listString:StringBuffer = new StringBuffer()
    for (elem <- list) {
      listString.append(elem).append(",")
    }

    out.collect(key._1 + "," + key._2 + "," + listString.substring(0,listString.length()-1))
  }
}