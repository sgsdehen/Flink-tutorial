package com.ngt.streamingwithflink.util

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import java.util.Calendar
import java.util.concurrent.TimeUnit
import scala.util.Random

/**
 * @author ngt
 * @create 2021-05-07 21:29
 *         继承富函数自定义数据源
 */
class SensorSource extends RichParallelSourceFunction[SensorReading] {

  // 用于标识源代码是否继续运行
  var running: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()
    val taskIdx: Int = getRuntimeContext.getIndexOfThisSubtask
    var curFTemp = (1 to 10).map {
      i => ("sensor_" + (taskIdx * 10 + i), 65 + (rand.nextGaussian() * 20))
    }

    while (running) {
      curFTemp = curFTemp.map(t => (t._1, t._2 + (rand.nextGaussian() * 0.5)))
      val curTime: Long = Calendar.getInstance.getTimeInMillis
      curFTemp.foreach(t => sourceContext.collect(SensorReading(t._1, curTime, t._2)))

      TimeUnit.MICROSECONDS.sleep(100)
    }
  }

  override def cancel(): Unit = running = false
}
