package com.ngt.window;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * @author ngt
 * @create 2021-01-28 17:38
 * 不使用sum聚合，使用Reduce进行聚合
 */
public class CountWindowAllReduceDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8181);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);
        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        // 不分组，将整体当成一个组。并行度为1
        AllWindowedStream<Integer, GlobalWindow> window = nums.countWindowAll(4, 2);

        window.reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer value1, Integer value2) throws Exception {
                return value1 + value2;
            }
        }).print();
        env.execute();
    }
}
