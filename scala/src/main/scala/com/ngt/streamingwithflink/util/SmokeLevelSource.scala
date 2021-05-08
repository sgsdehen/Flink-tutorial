package com.ngt.streamingwithflink.util

import com.ngt.streamingwithflink.util.SmokeLevel.SmokeLevel
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import java.util.concurrent.TimeUnit
import scala.util.Random

/**
 * @author ngt
 * @create 2021-05-08 14:15
 */
class SmokeLevelSource extends RichParallelSourceFunction[SmokeLevel] {
  var running: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SmokeLevel]): Unit = {
    val rand = new Random()

    while (running) {
      if (rand.nextGaussian() > 0.8) sourceContext.collect(SmokeLevel.High)
      else sourceContext.collect(SmokeLevel.Low)

      TimeUnit.SECONDS.sleep(1)
    }
  }

  override def cancel(): Unit = running = false
}
