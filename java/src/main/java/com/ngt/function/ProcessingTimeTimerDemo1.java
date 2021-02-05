package com.ngt.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author ngt
 * @create 2021-01-31 7:10
 */
public class ProcessingTimeTimerDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> words = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                collector.collect(Tuple2.of(words[0], Integer.valueOf(words[1])));
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyed = words.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0;
            }
        });

        keyed.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private transient ValueState<Integer> counter;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>(" ", Integer.class);
                counter = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                // 注册整点定时器
                long currentProcessingTime = ctx.timerService().currentProcessingTime();
                System.out.println("定时器注册时间：" + currentProcessingTime + " 定时器触发时间：" + (currentProcessingTime + 10000));
                ctx.timerService().registerProcessingTimeTimer((currentProcessingTime / 60000 + 1) * 60000);

                Integer currentCount = value.f1;
                Integer historyCount = counter.value();
                if (historyCount == null) {
                    historyCount = 0;
                }
                historyCount += currentCount;
                counter.update(historyCount);
                value.f1 = historyCount;
                //out.collect(value);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                Integer value = counter.value();
                // 获取当前的 key 
                String currentKey = ctx.getCurrentKey();
                out.collect(Tuple2.of(currentKey, value));
            }
        }).print();

        env.execute();
    }
}
