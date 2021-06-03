package cn.bitnei

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.io.File
import scala.collection.mutable.ListBuffer

/**
 * @author ngt on 2021-06-02 16:02
 * @version 1.0
 *          当天第一出行时间
 */
object BMWFristTime {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val lines: DataStream[String] = env.readTextFile("data/当天第一次出行时间-非工作日.csv")

    val value: DataStream[String] = lines.map(data => {
      val arr: Array[String] = data.split(",")
      (arr(0), arr(1), arr(2).toInt, arr(3).toInt, System.currentTimeMillis())
    })
      .assignAscendingTimestamps(_._5)
      .map(data => FirstTime(data._1, data._2, data._3, data._4))
      .keyBy(data => (data.city, data.vehCommon))
      .window(TumblingEventTimeWindows.of(Time.seconds(20)))
      .process(new FirstTimeWindow)


    val filePath = "data/非工作日.csv"
    val file = new File(filePath)
    if (file.exists()) {
      file.delete()
    }

    value
      .map(data =>output(data))
      .writeAsCsv(filePath)

    value.print()
    env.execute()
  }
}

case class FirstTime(city: String, vehCommon: String, fistTime: Int, vehCount: Int)

case class output(result: String)

class FirstTimeWindow extends ProcessWindowFunction[FirstTime,String,(String,String),TimeWindow]{
  override def process(key: (String, String),
                       context: Context, elements: Iterable[FirstTime],
                       out: Collector[String]): Unit = {
    val list: ListBuffer[Int] = new ListBuffer[Int]
    for (i <- 0 to 23) {
      list += 0
    }

    val iterator: Iterator[FirstTime] = elements.iterator
    for (elem <- iterator){
      list(elem.fistTime) = elem.vehCount
    }
    val listString: StringBuffer = new StringBuffer()
    for (elem <- list) {
      listString.append(elem).append(",")
    }

    out.collect(key._1 + "," + key._2 + "," + listString.substring(0, listString.length() - 1))
  }
}