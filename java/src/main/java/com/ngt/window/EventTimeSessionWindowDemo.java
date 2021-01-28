package com.ngt.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author ngt
 * @create 2021-01-29 0:56
 */
public class EventTimeSessionWindowDemo {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8181);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        // 1609512630000,a,1
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> timeAndCount = lines.map(data -> {
            String[] split = data.split(",");
            return Tuple3.of(Long.valueOf(split[0]), split[1], Integer.valueOf(split[2]));
        }).returns(Types.TUPLE(Types.LONG, Types.STRING, Types.INT));

        // 不需要再使用  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); 指定使用 EventTime
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> operator =
                timeAndCount.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<Long, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f0))
                        .setParallelism(1);   // 将并行度设置为 1

        // 如果 operator 并行度大于1 那么每个窗口的时间满足条件才会触发窗口
        operator.keyBy(data -> data.f1).window(EventTimeSessionWindows.withGap(Time.seconds(5))).sum(2).print();
        env.execute();
    }
}

/*
1609512630000,a,1
1609512631000,a,2
1609512634000,b,5
1609512635000,b,11
1609512636000,a,2
1609512641000,b,9

2> (1609512634000,b,25)
6> (1609512630000,a,3)
 */