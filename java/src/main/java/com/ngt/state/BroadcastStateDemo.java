package com.ngt.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @author ngt
 * @create 2021-02-01 12:02
 * * 用户id，活动id，事件类型(1浏览，2参与)
 * * user1,A,1
 * * User1,A,1
 * * User1,A,2
 * * User2,A,1
 * * User2,A,2
 * * User3,A,2
 * * User1,B,1
 * * User1,B,2
 * * User2,B,1
 * * User3,A,1
 * * User3,A,1
 * * User3,B,1
 * * User4,A,1
 * * User4,A,1
 * * 统计各个活动，事件的人数和次数
 * <p>
 * INSERT,1,浏览
 * INSERT,2,参与
 * INSERT,3,消费
 * UPDATE,3,退出
 * 使用 BroadcastState 将事实表和维度表进行连接操作
 * https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/stream/state/broadcast_state.html
 */
public class BroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 接收事实输入 User4,A,1
        DataStreamSource<String> act = env.socketTextStream("192.168.31.8", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, String>> actStream = act.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String line) throws Exception {
                String[] split = line.split(",");
                return Tuple3.of(split[0], split[1], split[2]);
            }
        });

        KeyedStream<Tuple3<String, String, String>, Tuple2<String, String>> keyByStream =
                actStream.keyBy(value -> Tuple2.of(value.f1, value.f2), TypeInformation.of(new TypeHint<>() {
                }));

        // 接收维度输入 INSERT,1,浏览
        DataStreamSource<String> dic = env.socketTextStream("192.168.31.8", 9999);
        SingleOutputStreamOperator<Tuple3<String, String, String>> dicStream = dic.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple3.of(split[0], split[1], split[2]);
            }
        });

        MapStateDescriptor<String, String> stateDescriptor = new MapStateDescriptor<>("dic-state", String.class, String.class);
        BroadcastStream<Tuple3<String, String, String>> broadcastStream = dicStream.broadcast(stateDescriptor);

        keyByStream.connect(broadcastStream)
                .process(new MyBroadcastProcessFunc(stateDescriptor))
                .print();
        env.execute();

    }

    private static class MyBroadcastProcessFunc extends KeyedBroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>,
            Tuple3<String, String, String>, Tuple4<String, String, Long, Long>> {

        private MapStateDescriptor<String, String> stateDescriptor;

        public MyBroadcastProcessFunc() {
        }

        public MyBroadcastProcessFunc(MapStateDescriptor<String, String> stateDescriptor) {
            this.stateDescriptor = stateDescriptor;
        }

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


        /**
         * 处理输入的每条事实数据
         *
         * @param value
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processElement(Tuple3<String, String, String> value, ReadOnlyContext ctx, Collector<Tuple4<String, String, Long, Long>> out) throws Exception {
            ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(stateDescriptor);
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

            String actId = value.f2;
            String actName = broadcastState.get(actId);

            // 输出结果

            out.collect(Tuple4.of(value.f1, actName, historyCount, (long) user.size()));

        }

        /**
         * 处理输入的每条维度数据
         *
         * @param value
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processBroadcastElement(Tuple3<String, String, String> value, Context ctx, Collector<Tuple4<String, String, Long, Long>> out) throws Exception {
            String type = value.f0;
            String actId = value.f1;
            String actName = value.f2;

            BroadcastState<String, String> broadcastState = ctx.getBroadcastState(stateDescriptor);
            if ("DELETE".equals(type)) { // 删除
                broadcastState.remove(type);
            } else {
                broadcastState.put(actId, actName); // 写入
            }
            // 不需要更新
        }

    }
}


/*

INSERT,1,浏览
INSERT,2,参与
INSERT,3,消费
UPDATE,3,退出


user1,A,1
User1,A,1
User1,A,2
User2,A,1
User2,A,2
User3,A,2
User1,B,1
User1,B,2
User2,B,1
User3,A,1
User3,A,1
User3,B,1
User4,A,1
User4,A,1


 */