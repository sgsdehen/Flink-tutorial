package com.ngt.streamingwithflink;

import com.ngt.streamingwithflink.util.SensorReading;
import com.ngt.streamingwithflink.util.SensorSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Random;

/**
 * @author ngt
 * @create 2021-05-19 20:52
 */
public class _08_lateReadingsOutput {
    static OutputTag<SensorReading> lateReadingsOutput = new OutputTag<>("late-readings");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(500L);

        // 1. ReduceFunction 使用lambda表达式
        SingleOutputStreamOperator<SensorReading> outOfOrderReadings = env.addSource(new SensorSource())
                .map(new TimestampShuffler(7 * 1000))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp));

        // 1. 使用ProcessFunction过滤出延迟读数(到侧输出)
        filterLateReadings(outOfOrderReadings);
        // 2. 将窗口操作符中的迟到读数重定向到侧输出
        sideOutputLateEventsWindow(outOfOrderReadings);
        // 3. 在窗口操作符中接收到延迟读数时更新结果
        updateForLateEventsWindow(outOfOrderReadings);
        env.execute();
    }

    private static void filterLateReadings(SingleOutputStreamOperator<SensorReading> readings) {
        SingleOutputStreamOperator<SensorReading> filteredReadings = readings.process(new LateReadingsFilter());
        DataStream<SensorReading> lateReadings = filteredReadings.getSideOutput(lateReadingsOutput);

        lateReadings.print();
        lateReadings
                .map(r -> "*** late reading *** " + r.id)
                .print();
    }

    private static void sideOutputLateEventsWindow(SingleOutputStreamOperator<SensorReading> readings) {

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> countPer10Secs = readings
                .keyBy(data -> data.id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sideOutputLateData(lateReadingsOutput)
                .process(new ProcessWindowFunction<SensorReading, Tuple3<String, Long, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<SensorReading> elements, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        int cnt = 0;
                        for (SensorReading element : elements) {
                            cnt++;
                        }
                        out.collect(Tuple3.of(s, context.window().getEnd(), cnt));
                    }
                });
        countPer10Secs.getSideOutput(lateReadingsOutput)
                .map(r -> "*** late reading *** " + r.id)
                .print();

        countPer10Secs.print();
    }

    private static void updateForLateEventsWindow(SingleOutputStreamOperator<SensorReading> readings) {
        SingleOutputStreamOperator<Tuple4<String, Long, Integer, String>> countPer10Secs = readings
                .keyBy(data -> data.id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(5))
                .process(new UpdatingWindowCountFunction());

        countPer10Secs.print();
    }

    static class LateReadingsFilter extends ProcessFunction<SensorReading, SensorReading> {
        @Override
        public void processElement(SensorReading value,
                                   Context ctx,
                                   Collector<SensorReading> out) throws Exception {
            if (value.temperature < ctx.timerService().currentWatermark()) {
                ctx.output(_08_lateReadingsOutput.lateReadingsOutput, value);
            } else {
                out.collect(value);
            }
        }
    }

    static class UpdatingWindowCountFunction extends ProcessWindowFunction<SensorReading, Tuple4<String, Long, Integer, String>, String, TimeWindow> {
        @Override
        public void process(String s,
                            Context context,
                            Iterable<SensorReading> elements,
                            Collector<Tuple4<String, Long, Integer, String>> out) throws Exception {
            int cnt = 0;
            for (SensorReading element : elements) {
                cnt++;
            }
            ValueState<Boolean> isUpdate =
                    context.windowState().getState(new ValueStateDescriptor<Boolean>("isUpdate", Boolean.class));

            if (!isUpdate.value()) {
                out.collect(Tuple4.of(s, context.window().getEnd(), cnt, "first"));
            } else {
                out.collect(Tuple4.of(s, context.window().getEnd(), cnt, "update"));
            }
        }
    }

    static class TimestampShuffler implements MapFunction<SensorReading, SensorReading> {
        private int maxRandomOffset;

        public TimestampShuffler(int maxRandomOffset) {
            this.maxRandomOffset = maxRandomOffset;
        }

        public TimestampShuffler() {
        }

        Random rand = new Random();

        @Override
        public SensorReading map(SensorReading value) throws Exception {
            long shuffleTs = value.timestamp + rand.nextInt(this.maxRandomOffset);
            return new SensorReading(value.id, shuffleTs, value.temperature);
        }
    }
}
