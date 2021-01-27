package com.ngt.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ngt
 * @create 2021-01-27 21:14
 */
public class MinMaxDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //spark,10
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndNum = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                String word = fields[0];
                int num = Integer.parseInt(fields[1]);
                return Tuple2.of(word, num);
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndNum.keyBy(data -> data.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> resMin = keyed.min(1);
        resMin.print();

        SingleOutputStreamOperator<Tuple2<String, Integer>> resMax = keyed.max(1);
        resMax.print();

        env.execute("MaxDemo");
    }
}
