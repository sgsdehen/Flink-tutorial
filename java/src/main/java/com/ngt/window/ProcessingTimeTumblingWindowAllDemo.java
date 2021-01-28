package com.ngt.window;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author ngt
 * @create 2021-01-28 19:45
 * 不分组，滚动窗口
 */
public class ProcessingTimeTumblingWindowAllDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8181);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);
        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        // 不分组，将整体当成一个组。并行度为, ProcessingTime 每5秒生成一个窗口
        nums.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum(0).print();
        env.execute();
    }
}
