package com.ngt.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ngt
 * @create 2021-01-30 21:51
 */
public class RestartStrategyDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

        // 未设置重启策略的时候程序出现异常就会退出，设置重启策略
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));

        // 10s一次Checkpointing，开启Checkpointing之后默认的重启策略就是无限重启
        // env.enableCheckpointing(10000);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(30), Time.seconds(3)));
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

        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyed.sum(1);
        summed.print();
        env.execute("StreamWordCount");
    }
}

/*
    https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/task_failure_recovery.html
                  无重启

    Fixed Delay Restart Strategy
    固定延时重启策略按照给定的次数尝试重启作业。 如果尝试超过了给定的最大次数，作业将最终失败。
    在连续的两次重启尝试之间，重启策略等待一段固定长度的时间。
    RestartStrategies.fixedDelayRestart(3, 5000) 最多重启3次，每5s一次

    Failure Rate Restart Strategy
    故障率重启策略在故障发生之后重启作业，但是当故障率（每个时间间隔发生故障的次数）超过设定的限制时，
    作业会最终失败。 在连续的两次重启尝试之间，重启策略等待一段固定长度的时间。
    RestartStrategies.failureRateRestart(3, Time.seconds(30), Time.seconds(3)) 30秒内错误达到 3 次就终止，超过30秒之后又重新开始计数
    30s 内最多重启3次，每次间隔3秒，超过30秒就可以从头开始

    No Restart Strategy
    作业直接失败，不尝试重启。
    RestartStrategies.noRestart()
    全局策略 fink-conf.yaml

    enableCheckpointing(10000);
    10s一次Checkpointing，开启Checkpointing之后默认的重启策略就是无限重启
 */