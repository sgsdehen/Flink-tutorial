package com.ngt.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;


/**
 * @author ngt
 * @create 2021-01-23 22:09
 */
public class LambdaStreamWordCount {
    public static void main(String[] args) throws Exception {
        // 手动创建本地运行环境，用于本地测试
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(8);
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);


        // 要使用 returns 指定返回的类型
        SingleOutputStreamOperator<String> word = lines.flatMap((String line, Collector<String> out) ->
                Arrays.stream(line.split(" ")).forEach(out::collect)).returns(Types.STRING);


        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne =
                word.map(w -> Tuple2.of(w, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));

        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = wordAndOne.keyBy(f -> f.f0).sum(1);
        summed.print();

        env.execute("LambdaStreamWordCount");
    }
}
