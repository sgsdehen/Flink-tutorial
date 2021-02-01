package com.ngt.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @author ngt
 * @create 2021-02-01 3:26
 * 用户id，活动id，事件类型(1浏览，2参与)
 * user1,A,1
 * User1,A,1
 * User1,A,2
 * User2,A,1
 * User2,A,2
 * User3,A,2
 * User1,B,1
 * User1,B,2
 * User2,B,1
 * User3,A,1
 * User3,A,1
 * User3,B,1
 * User4,A,1
 * User4,A,1
 * 统计各个活动，事件的人数和次数
 */
public class ActivityCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, String>> dataStream = lines.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String line) throws Exception {
                String[] split = line.split(",");
                return Tuple3.of(split[0], split[1], split[2]);
            }
        });

        // 根据活动和事件进行分组
        KeyedStream<Tuple3<String, String, String>, Tuple2<String, String>> keyBy =
                dataStream.keyBy( value -> Tuple2.of(value.f1, value.f2),TypeInformation.of(new TypeHint<>() {}));


        keyBy.process(new KeyedProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, Tuple4<String, String, Long, Long>>() {
            private transient ValueState<Long> countState;
            private transient ValueState<HashSet<String>> userState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Long> descriptor1 = new ValueStateDescriptor<>("state1", Long.class);
                countState = getRuntimeContext().getState(descriptor1);

                ValueStateDescriptor<HashSet<String>> descriptor2 = new ValueStateDescriptor<>("state2", TypeInformation.of(new TypeHint<HashSet<String>>() {
                }));
                userState = getRuntimeContext().getState(descriptor2);

            }

            @Override
            public void processElement(Tuple3<String, String, String> value, Context ctx, Collector<Tuple4<String, String, Long, Long>> out) throws Exception {
                // 计算次数
                Long historyCount = countState.value();
                if (historyCount == null) {
                    historyCount = 0L;
                }
                historyCount++;
                countState.update(historyCount);

                // 计算用户数
                HashSet<String> user = userState.value();
                if (user == null) {
                    user = new HashSet<>();
                }
                user.add(value.f0);
                userState.update(user);

                // 输出结果

                out.collect(Tuple4.of(value.f1, value.f2, historyCount, (long) user.size()));
            }
        }).print();
        env.execute();
    }
}
