package cn.bitnei

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * Created on 2021-06-01 22:50.
 *
 * @author ngt
 */
object BMWMileAvgMonth {
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
        (arr(18), arr(1), arr(37).toDouble, arr(17).toInt, System.currentTimeMillis())
      }).filter(_._4 != 0)
      .assignAscendingTimestamps(data => data._5)
      .map(data => MileMonth(data._1, data._2, data._3))
      .filter(data => cityList.contains(data.drive_city))
      .map(data => {
        if (data.mileAvgMonth < 500) data.mileAvgMonth = 0
        if (data.mileAvgMonth >= 500 && data.mileAvgMonth < 1000) data.mileAvgMonth = 500
        if (data.mileAvgMonth >= 1000 && data.mileAvgMonth < 1500) data.mileAvgMonth = 1000
        if (data.mileAvgMonth >= 1500 && data.mileAvgMonth < 2000) data.mileAvgMonth = 1500
        if (data.mileAvgMonth >= 2000 && data.mileAvgMonth < 2500) data.mileAvgMonth = 2000
        if (data.mileAvgMonth >= 2500 && data.mileAvgMonth < 3000) data.mileAvgMonth = 2500
        if (data.mileAvgMonth >= 3000 && data.mileAvgMonth < 3500) data.mileAvgMonth = 3000
        if (data.mileAvgMonth >= 3500 && data.mileAvgMonth < 4000) data.mileAvgMonth = 3500
        if (data.mileAvgMonth >= 4000 && data.mileAvgMonth < 4500) data.mileAvgMonth = 4000
        if (data.mileAvgMonth >= 4500) data.mileAvgMonth = 4500

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
      .process(new MileMonthWindow)
      .print()

    env.execute()
  }
}

case class MileMonth(
                    drive_city: String,
                    var veh_common_name: String,
                    var mileAvgMonth: Double
                  )


class MileMonthWindow extends ProcessWindowFunction[(MileMonth, Int), String, (String, String), TimeWindow] {
  override def process(key: (String, String),
                       context: Context,
                       elements: Iterable[(MileMonth, Int)],
                       out: Collector[String]): Unit = {
    val list: ListBuffer[Int] = new ListBuffer[Int]
    for (i <- 1 to 10) {
      list += 0
    }
    val iterator: Iterator[(MileMonth, Int)] = elements.iterator
    for (elem <- iterator) {
      list(elem._1.mileAvgMonth.toInt / 500) += elem._2
    }
    val listString: StringBuffer = new StringBuffer()
    for (elem <- list) {
      listString.append(elem).append(",")
    }

    out.collect(key._1 + "," + key._2 + "," + listString.substring(0, listString.length() - 1))
  }
}