package com.ngt.streamingwithflink.util

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author ngt
 * @create 2021-05-07 21:38
 */
class SensorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {
  override def extractTimestamp(t: SensorReading): Long = {
    t.timestamp
  }
}
