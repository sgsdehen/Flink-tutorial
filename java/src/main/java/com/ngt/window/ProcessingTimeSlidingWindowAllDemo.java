package com.ngt.window;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author ngt
 * @create 2021-01-28 20:36
 */
public class ProcessingTimeSlidingWindowAllDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8181);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);
        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        // 不分组，将整体当成一个组。并行度为, ProcessingTime 10 秒钟窗口，每5秒钟滑动一次
        nums.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum(0).print();
        env.execute();
    }
}
