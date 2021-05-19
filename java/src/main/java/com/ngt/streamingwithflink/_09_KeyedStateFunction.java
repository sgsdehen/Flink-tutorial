package com.ngt.streamingwithflink;

import com.ngt.streamingwithflink.util.SensorReading;
import com.ngt.streamingwithflink.util.SensorSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple4;

import java.time.Duration;

/**
 * @author ngt
 * @create 2021-05-19 21:57
 */
public class _09_KeyedStateFunction {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(500L);

        SingleOutputStreamOperator<SensorReading> sensorData = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                            @Override
                            public long extractTimestamp(SensorReading element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        sensorData
                .keyBy(data -> data.id)
                .flatMap(new TemperatureAlertFunction(1.7))
                .print();

        // 利用flatMapState实现  java未提供该实现
    }


    // 使用flatMap是因为可能没有输出
    static class TemperatureAlertFunction extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {
        private Double threshold;

        public TemperatureAlertFunction(Double threshold) {
            this.threshold = threshold;
        }

        public TemperatureAlertFunction() {
        }

        private transient ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", Double.class));
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double lastTmep = lastTempState.value();
            Double tempDiff = Math.abs(value.temperature - lastTmep);

            if (tempDiff > threshold) {
                out.collect(Tuple3.of(value.id, value.temperature, tempDiff));
            }
        }
    }
}
