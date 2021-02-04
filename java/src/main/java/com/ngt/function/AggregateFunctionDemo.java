package com.ngt.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author ngt
 * @create 2021-02-04 23:38
 */
public class AggregateFunctionDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);
        SingleOutputStreamOperator<Tuple2<String, Long>> words = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] words = s.split(" ");
                collector.collect(Tuple2.of(words[0], Long.valueOf(words[1])));
            }
        });

        words.keyBy(data -> data.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new AverageAggregate())
                .print();
        env.execute();

    }


    // 对窗口的数据分组求平均值
    private static class AverageAggregate implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Tuple2<String,Double>> {

        private String key;
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return new Tuple2<>(0L, 0L);
        }

        @Override
        public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
            key = value.f0;
            return new Tuple2<>(accumulator.f0 + value.f1, accumulator.f1 + 1L);
        }

        @Override

        public Tuple2<String,Double> getResult(Tuple2<Long, Long> accumulator) {
            return Tuple2.of(key,((double) accumulator.f0) / accumulator.f1);
        }

        // 非 SessionWindow，可以不实现
        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }
}

/*

flink 1
flink 2
flink 3
flink 4
spark 1
hadoop 2
spark 3
hadoop 21

8> (hadoop,11.5)
7> (flink,2.5)
1> (spark,2.0)
 */