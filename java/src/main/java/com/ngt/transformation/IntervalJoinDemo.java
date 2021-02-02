package com.ngt.transformation;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Arrays;

/**
 * @author ngt
 * @create 2021-02-03 0:30
 */
public class IntervalJoinDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8181);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.enableCheckpointing(10000);

        // DataStream1
        DataStreamSource<String> lines1 = env.socketTextStream("192.168.31.8", 8888);

        SingleOutputStreamOperator<String> operator1 =
                lines1.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((line, timestamp) -> Long.parseLong(line.split(",")[0])))
                        .setParallelism(1);   // 将并行度设置为 1

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne1 = operator1.map(data -> {
            String[] split = data.split(",");
            return Tuple2.of(split[1], Integer.valueOf(split[2]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));


        // DataStream2
        DataStreamSource<String> lines2 = env.socketTextStream("192.168.31.8", 9999);

        SingleOutputStreamOperator<String> operator2 =
                lines2.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((line, timestamp) -> Long.parseLong(line.split(",")[0])))
                        .setParallelism(1);   // 将并行度设置为 1

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne2 = operator2.map(data -> {
            String[] split = data.split(",");
            return Tuple2.of(split[1], Integer.valueOf(split[2]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        wordAndOne1.keyBy(data -> data.f0)
                .intervalJoin(wordAndOne2.keyBy(data -> data.f0))
//                .inEventTime()
                .between(Time.seconds(-2), Time.seconds(2))
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> left, Tuple2<String, Integer> right,
                                               Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(Tuple2.of(left.f0, left.f1 + right.f1));
                    }
                }).print();


        env.execute();
    }
}

/*
1609512632000,a,2
1609512630000,a,9
(a,11)
1609512632000,b,1
1609512634000,b,3
(b,4)
1609512641000,d,1
1609512644000,d,6
没有输出

 */