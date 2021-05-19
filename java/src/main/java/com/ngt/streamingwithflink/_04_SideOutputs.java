package com.ngt.streamingwithflink;

import com.ngt.streamingwithflink.util.SensorReading;
import com.ngt.streamingwithflink.util.SensorSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author ngt
 * @create 2021-05-17 22:50
 */
public class _04_SideOutputs {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setAutoWatermarkInterval(1000L);
        env.getCheckpointConfig().setCheckpointInterval(10 * 1000L);

        SingleOutputStreamOperator<SensorReading> sensorData = env
                .addSource(new SensorSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                            @Override
                            public long extractTimestamp(SensorReading element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        sensorData.process(new FreezingMonitor())
                .print();

        env.execute();
    }

    private static  class FreezingMonitor extends ProcessFunction<SensorReading, SensorReading> {
        private final OutputTag<String> freezingAlarmOutput = new OutputTag<>("freezing-alarms");

        @Override
        public void processElement(SensorReading value,
                                   Context ctx,
                                   Collector<SensorReading> out) throws Exception {
            if (value.temperature < 32.0) {
                ctx.output(freezingAlarmOutput, "Freezing Alarm for " + value.id + " temperature is " + value.temperature);
            }
            out.collect(value);
        }
    }
}

