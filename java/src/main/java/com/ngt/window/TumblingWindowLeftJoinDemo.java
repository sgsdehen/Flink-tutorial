package com.ngt.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author ngt
 * @create 2021-01-29 3:09
 * 使用左外连接，第一个流没有join上也要输出
 */
public class TumblingWindowLeftJoinDemo {
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
        leftStream.coGroup(rightStream)
                .where(value -> value.f1)
                .equalTo(value -> value.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple3<Long, String, Integer>, Tuple3<Long, String, Integer>,
                        Tuple5<Long, String, Integer, Long, Integer>>() {
                    @Override
                    public void coGroup(Iterable<Tuple3<Long, String, Integer>> first,
                                        Iterable<Tuple3<Long, String, Integer>> second,
                                        Collector<Tuple5<Long, String, Integer, Long, Integer>> out) throws Exception {
                        // 当两个窗口满足条件就会触发
                        // 方法中输入的数据是同一个分区中的数据
                        // 如果第一个迭代器中有数据，第二个迭代器中有数据join
                        boolean joined = false;
                        for (Tuple3<Long, String, Integer> tp1 : first) {
                            for (Tuple3<Long, String, Integer> tp2 : second) {
                                joined = true;
                                out.collect(Tuple5.of(tp1.f0, tp1.f1, tp1.f2, tp2.f0, tp2.f2));
                            }
                        }
                        // 如果第一个迭代器中有数据，第二个迭代器中没有数据 leftOutJoin
                        if (!joined) {
                            for (Tuple3<Long, String, Integer> tp1 : first) {
                                out.collect(Tuple5.of(tp1.f0, tp1.f1, tp1.f2, null, null));
                            }
                        }

                    }
                }).print();

        env.execute();
    }
}


