package com.ngt.window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ngt
 * @create 2021-01-28 19:27
 * 先进行keyby再进行窗口的划分
 */
public class CountWindowDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8181);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // saprk,1
        // scala,9
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lines.map(data -> {
            String[] split = data.split(",");
            return Tuple2.of(split[0], Integer.valueOf(split[1]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndCount.keyBy(t -> t.f0);
        // 组内增量聚合，组内达到指定的条数就进行触发输出
        keyed.countWindow(3).sum(1).print();
        env.execute();
    }
}
