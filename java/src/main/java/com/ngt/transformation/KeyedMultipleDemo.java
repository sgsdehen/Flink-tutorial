package com.ngt.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ngt
 * @create 2021-01-27 16:49
 * 同时使用多个字段作为使用的工具
 */
public class KeyedMultipleDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

        // 输入：省份,城市,金额
        SingleOutputStreamOperator<Tuple3<String, String, Double>> provinceCityAndMoney = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] words = value.split(",");
                return Tuple3.of(words[0], words[1], Double.valueOf(words[2]));
            }
        });

        // 1. 同时使用多个字段作为分组的依据
        provinceCityAndMoney.keyBy(0, 1).sum(2).print();

        // 2. 使用 KeySelector, 将两个字段相加
        provinceCityAndMoney.keyBy(new KeySelector<Tuple3<String, String, Double>, String>() {
            @Override
            public String getKey(Tuple3<String, String, Double> value) throws Exception {
                return value.f0 + value.f1;
            }
        }).sum(2).print();

        // 3. 使用 KeySelector, 将两个字段构成一个元组
        provinceCityAndMoney.keyBy(new KeySelector<Tuple3<String, String, Double>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, Double> value) throws Exception {
                return Tuple2.of(value.f0, value.f1);
            }
        }).sum(2).print();

        // 4. 使用 lambda 表达式，将两个字段相加
        provinceCityAndMoney.keyBy(value -> value.f0 + value.f1).sum(2).print();

        // 5. 使用 lambda 表达式，将两个字段构成一个元组
        provinceCityAndMoney.keyBy(value -> Tuple2.of(value.f0, value.f1)).sum(2).
                returns(Types.TUPLE(Types.STRING, Types.STRING, Types.DOUBLE)).print();
        env.execute();
    }
}
