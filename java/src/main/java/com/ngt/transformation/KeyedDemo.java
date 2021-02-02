package com.ngt.transformation;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ngt
 * @create 2021-01-27 16:38
 */
public class KeyedDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

        // keyby 需要 Tuple 类型，需要 returns 指定类型
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = lines.map(w -> Tuple2.of(w, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        // 1. 使用下标，只适用于元组，新版中已过时
        map.keyBy(0).sum(1).print();

        // 2. 使用 KeySelector
        map.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).sum(1).print();

        // 3. 使用 lambda 表达式，此时类型可以省略
        map.keyBy(d -> d.f0).sum(1).print();
        env.execute();
    }
}

/*
flink
spark
flink
 */