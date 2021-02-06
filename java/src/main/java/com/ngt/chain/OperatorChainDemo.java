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
 * @create 2021-01-29 22:56
 * https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/stream/operators/#%E7%AE%97%E5%AD%90%E9%93%BE%E5%92%8C%E8%B5%84%E6%BA%90%E7%BB%84
 */
public class OperatorChainDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8181);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

        // 系统默认开启 OperatorChaining ，可以手动关闭
        env.disableOperatorChaining();
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
                return value.startsWith("error");
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = filterd.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = wordAndOne.keyBy(data -> data.f0).sum(1);
        sumed.print();
        env.execute();

    }
}
/*

    Source: Socket Stream     (Parallelism：1)  --rebalance-->
    Flat Map -> Filter -> Map (Parallelism：8)  --hash-->
    Keyed Aggregation -> Sink: Print to Std. Out(Parallelism：8)

    算子链，默认情况下处于开启状态
    Flink 将算子的 subtasks 链接成 tasks。每个 task 由一个线程执行。将算子链接成 task 是个有用的优化：
    它减少线程间切换、缓冲的开销，并且减少延迟的同时增加整体吞吐量。

    当出现下列情形的时候会切换算子链：
    1. 并行度发生变化
    2. 发生物理分区的分区的时候，如：shuffle，Rebalancing，Rescaling 等
 */