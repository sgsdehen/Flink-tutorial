package com.ngt.transformation;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ngt
 * @create 2021-01-28 2:15
 */
public class ProjectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.fromElements("zs,18,9000", "ls,23,6534");

        SingleOutputStreamOperator<Tuple3<String, Integer, Double>> maped = lines.map(e -> {
            String[] split = e.split(",");
            return Tuple3.of(split[1], Integer.valueOf(split[1]), Double.valueOf(split[2]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT, Types.DOUBLE));

        maped.project(2, 0).print();
        // 使用 map 实现
        maped.map(data -> new Tuple2(data.f2, data.f0)).
                returns(Types.TUPLE(Types.DOUBLE, Types.STRING)).print();
        env.execute();
    }
}
