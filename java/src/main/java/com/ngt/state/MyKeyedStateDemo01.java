package com.ngt.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @author ngt
 * @create 2021-01-30 22:51
 */
public class MyKeyedStateDemo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

        // 未设置重启策略的时候程序出现异常就会退出，设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));

        SingleOutputStreamOperator<Tuple2<String, Integer>> words = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    if ("error".equals(word)) {
                        throw new RuntimeException("Exception");
                    }
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });


        KeyedStream<Tuple2<String, Integer>, String> keyed = words.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0;
            }
        });

        // 不调用 sum，因为其会存在状态,自己实现状态存储， 使用 RichMapFunction 便于获取运行时上下文
        // 注意此方法不能实现状态的保存，无论是否启用 enableCheckpointing，因为该方法没有将数据持久化存储
        keyed.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            // 用于存储中间结果
            private HashMap<String, Integer> counter = new HashMap<>();

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> input) throws Exception {
                String word = input.f0;
                Integer count = input.f1;
                Integer historyCount = counter.get(word);
                if(historyCount == null){
                    historyCount = 0;
                }
                int sum = historyCount + count;
                // 更新 map 中的数据
                counter.put(word, sum);
                // 输出结果
                return Tuple2.of(word,sum);
            }
        }).print();

        env.execute();
    }
}
