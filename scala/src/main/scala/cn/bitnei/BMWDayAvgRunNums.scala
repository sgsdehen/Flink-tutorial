package cn.bitnei

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * Created on 2021-06-01 23:27.
 *
 * @author ngt
 *         每天行驶次数统计
 */
object BMWDayAvgRunNums {
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
        (arr(18), arr(1), arr(22).toInt, arr(17).toInt, System.currentTimeMillis())
      }).filter(_._4 != 0)
      .assignAscendingTimestamps(data => data._5)
      .map(data => DayRunNums(data._1, data._2, data._3))
      .filter(data => cityList.contains(data.drive_city))
      .map(data => {
        if (data.dayAvgRunNums > 9) data.dayAvgRunNums = 9
        data
      })
      .filter(_.dayAvgRunNums != 0)
      .map((_, 1))
      .keyBy(data => (data._1.drive_city, data._1.veh_common_name))
      .window(TumblingEventTimeWindows.of(Time.seconds(20)))
      .process(new DayRunNumsWindow)
      .print()

    env.execute()
  }
}


case class DayRunNums(
                       drive_city: String,
                       var veh_common_name: String,
                       var dayAvgRunNums: Int
                     )


class DayRunNumsWindow extends ProcessWindowFunction[(DayRunNums, Int), String, (String, String), TimeWindow] {
  override def process(key: (String, String),
                       context: Context,
                       elements: Iterable[(DayRunNums, Int)],
                       out: Collector[String]): Unit = {
    val list: ListBuffer[Int] = new ListBuffer[Int]
    for (i <- 1 to 9) {
      list += 0
    }
    val iterator: Iterator[(DayRunNums, Int)] = elements.iterator
    for (elem <- iterator) {
      list(elem._1.dayAvgRunNums - 1) += elem._2
    }
    val listString: StringBuffer = new StringBuffer()
    for (elem <- list) {
      listString.append(elem).append(",")
    }
    out.collect(key._1 + "," + key._2 + "," + listString.substring(0, listString.length() - 1))
  }
}