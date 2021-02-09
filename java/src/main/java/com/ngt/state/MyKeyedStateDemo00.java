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
 * @create 2021-01-31 1:50
 */
public class MyKeyedStateDemo00 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));
        env.enableCheckpointing(5000);

        // 验证异常发生之后状态是否能够保存
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
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


        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0;
            }
        });

        // 因为此处只是保存了一个value没有保存 key，所以会将一个subtask中的所有进行累加操作，而不是分成
        // 不同的 key
        keyed.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            // 定义一个成员变量，
            private Integer counter = 0;

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> input) throws Exception {
                Integer currentCount = input.f1;
                counter += currentCount;
                input.f1 = counter;
                return input;
            }
        }).print();

        env.execute();
    }
}

/*
spark
saprk
flink flink
flink
hadoop
hadoop
hive


1> (spark,1)
2> (saprk,1)
7> (flink,1)
7> (flink,2)
7> (flink,3)
8> (hadoop,1)
8> (hadoop,2)
1> (hive,2)  hive会接着 (spark,1)进行累加所以是 (hive,2)
 */