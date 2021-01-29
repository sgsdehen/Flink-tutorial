package com.ngt.chain;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ngt
 * @create 2021-01-29 23:19
 */
public class StartNewChainDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8181);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

        // 系统默认开启 OperatorChaining ，可以手动关闭
        // env.disableOperatorChaining();
        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(",");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        SingleOutputStreamOperator<String> filterd = words.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("reeor");
            }
        });

        // 从Map开启一个新的算子链，可以将计算密集的任务单独放置于一个资源槽中
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = filterd.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).startNewChain();

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = wordAndOne.keyBy(data -> data.f0).sum(1);
        sumed.print();
        env.execute();
    }
}
/*
    Source: Socket Stream  (Parallelism：1)  --rebalance-->
    Flat Map -> Filter     (Parallelism：8)  --forward-->
    Map                    (Parallelism：8)  --hash-->
    Keyed Aggregation -> Sink: Print to Std. Out(Parallelism：8)
 */
