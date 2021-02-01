package com.ngt.transformation;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author ngt
 * @create 2021-01-28 1:27
 */
public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8181);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<String> lines1 = env.socketTextStream("192.168.31.8", 8888);
        DataStreamSource<String> lines2 = env.socketTextStream("192.168.31.8", 9999);

        SingleOutputStreamOperator<Double> map = lines2.map(w -> Double.parseDouble(w));

        // 允许两个流之间共享状态
        ConnectedStreams<String, Double> connect = lines1.connect(map);

        // 流1的类型，流2的类型，生成新流的类型
        SingleOutputStreamOperator<String> result = connect.map(new CoMapFunction<String, Double, String>() {
            @Override
            public String map1(String value) throws Exception {
                return value.toUpperCase();
            }

            @Override
            public String map2(Double value) throws Exception {
                return value * 10 + "";
            }
            // 两个流的map方法执行完的返回值会放入到新的流中


        });
        result.print();
        env.execute();
    }
}
