package com.ngt.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ngt
 * @create 2021-01-28 1:49
 */
public class IterateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

        SingleOutputStreamOperator<Long> numbers = lines.map(Long::parseLong);

        // 调用 iterate 方法 DataStrea -> IterativeStream
        IterativeStream<Long> iteration = numbers.iterate();

        // 迭代操作
        SingleOutputStreamOperator<Long> iterationBody = iteration.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("iterate input =>" + value);
                return (value -= 2);
            }
        });

        // 继续迭代的条件
        SingleOutputStreamOperator<Long> feedback = iterationBody.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 0;
            }
        });

        // 传入迭代的条件
        iteration.closeWith(feedback);

        // 退出迭代的条件
        SingleOutputStreamOperator<Long> output = iterationBody.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value <= 0;
            }
        });

        output.print("output value");
        env.execute();

    }
}

/*
    iterate input =>9
    iterate input =>7
    iterate input =>5
    iterate input =>3
    iterate input =>1
    output value:1> -1

    iterate input =>8
    iterate input =>6
    iterate input =>4
    iterate input =>2
    output value:2> 0
 */