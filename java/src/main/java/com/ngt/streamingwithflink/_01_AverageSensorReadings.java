package com.ngt.streamingwithflink;

import com.ngt.streamingwithflink.util.SensorReading;
import com.ngt.streamingwithflink.util.SensorSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;
import java.time.Duration;

/**
 * @author ngt
 * @create 2021-05-17 21:28
 */
public class _01_AverageSensorReadings {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<SensorReading> sensorData = env
                .addSource(new SensorSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<SensorReading>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                            @Override
                            public long extractTimestamp(SensorReading element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));


        env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <SensorReading>forMonotonousTimestamps()
                        .withTimestampAssigner((data, timestamp) -> data.timestamp));


        sensorData
                .map(value -> new SensorReading(value.id, value.timestamp, (value.temperature - 32) * (5.0 / 9.0)))
                .keyBy(data -> data.id)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .apply(new TemperatureAverager())
                .print();

        env.execute("Compute average sensor temperature");

    }


    private static class TemperatureAverager implements WindowFunction<SensorReading, SensorReading, String, TimeWindow> {
        @Override
        public void apply(String s,
                          TimeWindow window,
                          Iterable<SensorReading> input,
                          Collector<SensorReading> out) throws Exception {

            int cnt = 0;
            double sum = 0.0;
            for (SensorReading r : input) {
                cnt++;
                sum += r.temperature;
            }
            double avgTemp = sum / cnt;

            out.collect(new SensorReading(s, window.getEnd(), avgTemp));
        }
    }

}

