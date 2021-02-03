package com.ngt.window;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author ngt
 * @create 2021-01-28 21:43
 *
 */
public class EvenTimeTumblingWindowAllDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8181);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

        SingleOutputStreamOperator<Tuple2<Long, Integer>> timeAndCount = lines.map(data -> {
            String[] split = data.split(",");
            return Tuple2.of(Long.valueOf(split[0]), Integer.valueOf(split[1]));
        }).returns(Types.TUPLE(Types.LONG, Types.INT));

        SingleOutputStreamOperator<Tuple2<Long, Integer>> operator =
                timeAndCount.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<Long, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f0))
                        .setParallelism(1);    // 将并行度设置为1


        // 不分组，将整体当成一个组。并行度为, ProcessingTime 每5秒生成一个窗口
        operator.windowAll(TumblingEventTimeWindows.of(Time.seconds(5))).sum(1).print();
        env.execute();
    }
}

/*
 老版本 API 使用步骤
 1. 设置EventTime为时间标准
 2. 提取数据中对应的时间，转换成 Long 类型的时间戳
 */

/*
输入
1609512630000,1
1609512631000,2
1609512634000,5
1609512634998,3
1609512634999,7
1609512635000,11 触发
1609512639999,16
1609512640000,18 触发

输出
(1609512630000,18) 1+2+5+3+7
(1609512635000,27) 11+16
闭区间 一个窗口中包含， [0000,4999]， 但是需要 5000 才能触发
 */