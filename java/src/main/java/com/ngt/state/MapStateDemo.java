package com.ngt.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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

/**
 * @author ngt
 * @create 2021-01-31 2:31
 */
public class MapStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));
        env.enableCheckpointing(5000);

        // 辽宁省,大连市,3000
        // 辽宁省,沈阳市,2300
        // 山东省,青岛市,3400
        SingleOutputStreamOperator<Tuple3<String, String, Double>> tpDataStream = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] words = value.split(",");
                return Tuple3.of(words[0], words[1], Double.valueOf(words[2]));
            }
        });

        KeyedStream<Tuple3<String, String, Double>, String> keyedStream = tpDataStream.keyBy(t -> t.f1);
        keyedStream.process(new KeyedProcessFunction<String, Tuple3<String, String, Double>, Tuple3<String, String, Double>>() {

            private transient MapState<String, Double> mapState; //不参与序列化，不使用序列化赋值

            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<String, Double> kvstate = new MapStateDescriptor<>("kvstate", String.class, Double.class);
                mapState = getRuntimeContext().getMapState(kvstate);
            }

            @Override
            public void processElement(Tuple3<String, String, Double> value, Context ctx, Collector<Tuple3<String, String, Double>> out) throws Exception {
                String city = value.f1;
                Double money = value.f2;
                Double historyMoney = mapState.get(city);
                if (historyMoney == null) {
                    historyMoney = 0.0;
                }
                historyMoney += money;
                // 更新状态
                mapState.put(city, historyMoney);
                value.f2 = historyMoney;
                out.collect(value);
            }
        }).print();
        env.execute();
    }
}

//
//
//
/*
辽宁省,大连市,3000
辽宁省,沈阳市,2300
山东省,青岛市,3400
辽宁省,大连市,2300
山东省,青岛市,1240
 */