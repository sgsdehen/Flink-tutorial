package com.ngt.streamingwithflink;

import com.ngt.streamingwithflink.util.SensorReading;
import com.ngt.streamingwithflink.util.SensorSource;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author ngt
 * @create 2021-05-17 21:56
 */
public class _02_WatermarkGeneration {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> readings = env.addSource(new SensorSource());

        // 1. 自定义 WatermarkGenerator 实现
        readings.assignTimestampsAndWatermarks((WatermarkStrategy<SensorReading>) context -> new PeriodicAssigner())
                .print();

        readings.assignTimestampsAndWatermarks((WatermarkStrategy<SensorReading>) context -> new PunctuatedAssigner())
                .print();

        // 2. 时间戳单调递增

        readings.assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                    @Override
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        // 3. 提前知道最大延迟
        readings.assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                    @Override
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        // 4. 处理空闲数据源
        readings.assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withIdleness(Duration.ofMinutes(1))); // 设置空闲超时时间


        env.execute();

    }

    // 周期性水位线分配器
    private static class PeriodicAssigner implements WatermarkGenerator<SensorReading> {

        private static final long maxOutOfOrderness = 60 * 1000L;
        private long currentMaxTimestamp = 0;

        @Override
        public void onEvent(SensorReading event, long eventTimestamp, WatermarkOutput output) {
            currentMaxTimestamp = Math.max(currentMaxTimestamp, event.timestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
        }
    }

// 定点水位分配器

   private static class PunctuatedAssigner implements WatermarkGenerator<SensorReading> {
        private static final long maxOutOfOrderness = 60 * 1000L;

        @Override
        public void onEvent(SensorReading event, long eventTimestamp, WatermarkOutput output) {
            if (event.id == "sensor_1") {
                output.emitWatermark(new Watermark(event.timestamp - maxOutOfOrderness - 1));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 无需实现
        }
    }
}


