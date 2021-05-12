package com.ngt.streamingwithflink

import com.ngt.streamingwithflink.util.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

/**
 * @author ngt
 * @create 2021-05-12 19:08
 */
object _08_lateReadingsOutput {
  val lateReadingsOutput: OutputTag[SensorReading] =
    new OutputTag[SensorReading]("late-readings")

  def main(args: Array[String]): Unit = {

  }

}


class LateReadingsFilter extends ProcessFunction[SensorReading, SensorReading] {
  override def processElement(
                               r: SensorReading,
                               ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                               out: Collector[SensorReading]): Unit = {

    if (r.timestamp < ctx.timerService().currentWatermark()) {
      ctx.output(_08_lateReadingsOutput.lateReadingsOutput, r)
    } else {
      out.collect(r)
    }
  }
}