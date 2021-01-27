package com.ngt.transformation;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ngt
 * @create 2021-01-27 20:16
 */
public class FoldDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> words = env.socketTextStream("192.168.31.8", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne =
                words.map(w -> Tuple2.of(w, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));

        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndOne.keyBy(data -> data.f0);

        // fold和reduce类似，可以指定一个初始值，在1.12 中已经删除
//        SingleOutputStreamOperator<String> result = keyed.fold("", new FoldFunction<Tuple2<String, Integer>, String>() {
//            @Override
//            public String fold(String accumulator, Tuple2<String, Integer> value) throws Exception {
//                String word = "";
//                int sum = 0;
//                if (accumulator.equals("")) {
//                    word = value.f0;
//                    sum += value.f1;
//                } else {
//                    String[] fields = accumulator.split("-");
//                    word = fields[0];
//                    sum = Integer.parseInt(fields[1]) + value.f1;
//                }
//
//                return word + "-" + sum;
//            }
//        });
//
//        result.print();


        env.execute("FoldDemo");

    }
}
