package com.ngt.function;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author ngt
 * @create 2021-02-01 2:07
 */
public class WindowProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1609512630000,a,1
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

        env.enableCheckpointing(10000);
        SingleOutputStreamOperator<String> operator =
                lines.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((line, timestamp) -> Long.valueOf(line.split(",")[0])))
                        .setParallelism(1);   // 将并行度设置为 1

        SingleOutputStreamOperator<Tuple2<String, Integer>> timeAndCount = operator.map(data -> {
            String[] split = data.split(",");
            return Tuple2.of(split[1], Integer.valueOf(split[2]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));


        // 不需要再使用  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); 指定使用 EventTime


        // 如果 operator 并行度大于1 那么每个窗口的时间满足条件才会触发窗口
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowed = timeAndCount.keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)));

        // process 和 apply 方法是类似的

        windowed.process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
            // 窗口触发，才会调用Process方法，该方法可以获取窗口内全量数据，数据缓存到 WindowState中
            @Override
            public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (Tuple2<String, Integer> element : elements) {
                    out.collect(element);
                }
            }
        }).print();
        env.execute();
    }

}
