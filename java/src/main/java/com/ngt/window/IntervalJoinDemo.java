package com.ngt.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author ngt
 * @create 2021-01-29 21:24
 * https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/stream/operators/joining.html
 */
public class IntervalJoinDemo {
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
        
        leftStream.keyBy(data->data.f1)
                .intervalJoin(rightStream.keyBy(data-> data.f1))
                .between(Time.seconds(-1), Time.seconds(1))
                .upperBoundExclusive() // 默认的区间是包含两端端点的，现在设置成前闭后开
                .process(new ProcessJoinFunction<Tuple3<Long, String, Integer>, Tuple3<Long, String, Integer>,
                        Tuple5<Long,Long,String,Integer,Integer>>() {
                    @Override
                    public void processElement(Tuple3<Long, String, Integer> left, Tuple3<Long, String, Integer> right, Context ctx, Collector<Tuple5<Long, Long, String, Integer, Integer>> out) throws Exception {
                        out.collect(Tuple5.of(left.f0,right.f0, left.f1, left.f2, right.f2));
                    }
                }).print();

        env.execute();
    }
}
