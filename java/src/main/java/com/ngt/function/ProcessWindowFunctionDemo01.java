package com.ngt.function;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
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
 * @create 2021-02-01 1:06
 * 在窗口中进行增量聚合，直接调用sum和reduce方法只会聚合窗口中的数据，不会聚合历史数据
 */
public class ProcessWindowFunctionDemo01 {
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


        windowed.reduce(new MyReduceFunction(), new MyWindowFunction()).print();
        env.execute();
    }

    private static class MyReduceFunction implements ReduceFunction<Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
            value1.f1 = value1.f1 + value2.f1;
            return value1;
        }
    }

    private static class MyWindowFunction extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {
        private transient ValueState<Integer> sumState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("wc", Integer.class);
            sumState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
            Integer historyCount = sumState.value();
            if (historyCount == null) {
                historyCount = 0;
            }

            Tuple2<String, Integer> tp = elements.iterator().next();

            tp.f1 += historyCount;
            // 必须要更新状态
            sumState.update(tp.f1);
            out.collect(tp);
        }
    }
}

/*
1609512630000,a,1
1609512631000,b,3
1609512632000,c,5
1609512633000,b,6
1609512635000,a,2
1609512636000,a,1
1609512638000,c,8
1609512639000,b,9
1609512640000,a,3


4> (c,5)
2> (b,9)
6> (a,1)
4> (c,13)
6> (a,4)
2> (b,18)
 */