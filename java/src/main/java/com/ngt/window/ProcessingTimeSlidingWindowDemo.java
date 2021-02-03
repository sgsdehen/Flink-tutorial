package com.ngt.window;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author ngt
 * @create 2021-01-28 20:57
 */
public class ProcessingTimeSlidingWindowDemo {
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

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowed =
                keyed.window(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)));

        windowed.sum(1).print();
        env.execute();
    }
}
