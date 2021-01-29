package com.ngt.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author ngt
 * @create 2021-01-29 2:21
 */
public class TumblingWindowJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<String> lines1 = env.socketTextStream("192.168.31.8", 8888);

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> leftInput = lines1.map(data -> {
            String[] split = data.split(",");
            return Tuple3.of(Long.valueOf(split[0]), split[1], Integer.valueOf(split[2]));
        }).returns(Types.TUPLE(Types.LONG, Types.STRING, Types.INT));


        DataStreamSource<String> lines2 = env.socketTextStream("192.168.31.8", 9999);

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> rightInput = lines2.map(data -> {
            String[] split = data.split(",");
            return Tuple3.of(Long.valueOf(split[0]), split[1], Integer.valueOf(split[2]));
        }).returns(Types.TUPLE(Types.LONG, Types.STRING, Types.INT));


        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> leftStream =
                leftInput.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<Long, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f0));

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> rightStream =
                rightInput.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<Long, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f0));

        // 注意两个流必须是相同的时间窗口才能join在一起
        // 两个流都触发的时候才会join
        leftStream.join(rightStream)
                .where(value -> value.f1)
                .equalTo(value -> value.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple3<Long, String, Integer>, Tuple3<Long, String, Integer>, Tuple5<Long,Long,String,Integer,Integer>>() {
                    @Override
                    public Tuple5<Long, Long, String, Integer, Integer> join(Tuple3<Long, String, Integer> first, Tuple3<Long, String, Integer> second) throws Exception {
                        return Tuple5.of(first.f0,second.f0, first.f1, first.f2, second.f2);
                    }
                }).print();

        env.execute();

    }
}

/*
1609512630000,a,1
1609512634999,a,4
1609512635000,b,11
1609512639999,c,16
1609512640000,b,18


1609512630010,a,10
1609512634999,a,40
1609512635000,b,110
1609512639999,c,160
1609512640000,b,180


(1609512630000,1609512630010,a,1,10)
(1609512630000,1609512634999,a,1,40)
(1609512634999,1609512630010,a,4,10)
(1609512634999,1609512634999,a,4,40)
(1609512635000,1609512635000,b,11,110)
(1609512639999,1609512639999,c,16,160)
 */