package com.ngt.function;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author ngt
 * @create 2021-02-01 0:40
 * 将迟到的数据使用测流输出
 */
public class WindowLateDateDemo {
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

        // 处理迟到数据
        OutputTag<Tuple3<Long, String, Integer>> lastoutputTag = new OutputTag<>("last") {
        };

        // 如果 operator 并行度大于1 那么每个窗口的时间满足条件才会触发窗口
        WindowedStream<Tuple3<Long, String, Integer>, String, TimeWindow> mainStream = operator.keyBy(data -> data.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(lastoutputTag);

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> sumed = mainStream.sum(2);
        sumed.print();
        // 主流中获取迟到的数据
        sumed.getSideOutput(lastoutputTag).print("lastdata");
        env.execute();
    }
}

/*
1609512630000,a,1
1609512631000,b,3
1609512632000,c,5
1609512633000,b,6
1609512635000,a,2
1609512636000,a,1
1609512638000,c,8
1609512639000,b,9
1609512640000,a,3

 */