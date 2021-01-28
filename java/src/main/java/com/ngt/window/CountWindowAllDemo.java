package com.ngt.window;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * @author ngt
 * @create 2021-01-28 17:16
 * 不分组划分窗口，按照数据的条数划分窗口，属于GlobalWindow
 */
public class CountWindowAllDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8181);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);
        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        // 不分组，将整体当成一个组。并行度为1
        AllWindowedStream<Integer, GlobalWindow> window = nums.countWindowAll(4, 2);

        // 增量聚合：累加是来一条数据加一条，因为只有一个分区能做到增量聚合，同时也避免大量保存数据的空间浪费
        window.sum(0).print();
        env.execute();
    }
}
