package com.ngt.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ngt
 * @create 2021-01-27 21:34
 * min 和 minBy的区别
 */
public class MinByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

        // 输入：身份，城市。金额
        SingleOutputStreamOperator<Tuple3<String, String, Double>> provinceCityAndMoney = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] words = value.split(",");
                return Tuple3.of(words[0], words[1], Double.valueOf(words[2]));
            }
        });
        /*
            统一输入
            A,b,3000
            A,d,2000
            A,f,2000
            B,b,1000
         */

        // min 不会更新非分区字段
        provinceCityAndMoney.keyBy(t -> t.f0).min(2).print();
        /*
        7> (A,b,3000.0)
        7> (A,b,2000.0)
        7> (A,b,2000.0)
        2> (B,b,1000.0)
         */

        // minBy 会更新非分区字段，会返回第一个具有最小值的元素
        provinceCityAndMoney.keyBy(t -> t.f0).minBy(2).print();
        /*
        7> (A,b,3000.0)
        7> (A,d,2000.0)
        7> (A,d,2000.0)
        2> (B,b,1000.0)
         */

        // minBy 会更新非分区字段，当第二次参数为 false 的时候，会返回最后个具有最小值的元素
        provinceCityAndMoney.keyBy((t -> t.f0)).minBy(2, false).print();
        /*
        7> (A,b,3000.0)
        7> (A,d,2000.0)
        7> (A,f,2000.0)
        2> (B,b,1000.0)
         */
        env.execute();
    }
}

/*
    min和minBy的区别在于，min返回的是参与分组的字段和要比较字段的最小值，
    如果数据中还有其他字段，其他字段的值是总是第一次输入的数据的值。而minBy返回的是要比较的最小值对应的全部数据。
 */