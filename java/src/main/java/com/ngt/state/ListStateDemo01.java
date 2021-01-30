package com.ngt.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.collection.mutable.ArrayLike;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ngt
 * @create 2021-01-31 2:56
 * <p>
 * user1,A
 * user1,B
 * user2,D
 * user2,C
 * ueer1,C
 * user1 -> (A,B,D)
 */
public class ListStateDemo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

        env.enableCheckpointing(5000);
        // 未设置重启策略的时候程序出现异常就会退出，设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));

        SingleOutputStreamOperator<Tuple2<String, String>> tpDataStream = lines.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] words = value.split(",");
                return Tuple2.of(words[0], words[1]);
            }
        });

        KeyedStream<Tuple2<String, String>, String> keyedStream = tpDataStream.keyBy(d -> d.f0);

        keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, List<String>>>() {
            private transient ListState<String> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("listState", String.class);
                listState = getRuntimeContext().getListState(listStateDescriptor);

            }

            @Override
            public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, List<String>>> out) throws Exception {
                String action = value.f1;
                listState.add(action);

                Iterable<String> iterator = listState.get();

                ArrayList<String> events = new ArrayList<>();
                for (String name : iterator) {
                    events.add(name);
                }
                // 输出
                out.collect(Tuple2.of(value.f0, events));
            }
        }).print();
        env.execute();
    }
}

/*
user1,A
user1,B
user2,D
user2,C
ueer1,C


6> (user1,[A])
6> (user1,[A, B])
1> (user2,[D])
1> (user2,[D, C])
7> (ueer1,[C])
 */