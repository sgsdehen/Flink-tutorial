package com.ngt.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ngt
 * @create 2021-02-01 3:06
 * 设置valuestate的存活时间
 */
public class KeyedStateTTLDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

        env.enableCheckpointing(5000);
        // 未设置重启策略的时候程序出现异常就会退出，设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));

        SingleOutputStreamOperator<Tuple2<String, Integer>> words = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    if ("error".equals(word)) {
                        throw new RuntimeException("Exception");
                    }
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });


        KeyedStream<Tuple2<String, Integer>, Object> keyed = words.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0;
            }
        });

        // Flink官方尽管可以使用一个value来保存状态，但是底层依然是Map类型
        // this.keyContext.getCurrentKey()  CopyOnWriteStateMap
        keyed.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            //transient声明一个实例变量，当对象存储时，它的值不需要维持,用transient关键字标记的成员变量不参与序列化过程。
            private transient ValueState<Integer> counter;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 定义一个状态TTLConfig
                StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(10)) // 时间是分不同的keyed的
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build();

                ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("wc", Integer.class);
                stateDescriptor.enableTimeToLive(ttlConfig);
                counter = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> input) throws Exception {
                Integer count = input.f1;
                // 获取当前Key对应的value的值
                Integer historyCount = counter.value();
                if (historyCount == null) {
                    historyCount = 0;
                }
                int total = historyCount + count;
                // 更新状态中的数据
                counter.update(total);
                // 输出结果
                input.f1 = total;
                return input;
            }
        }).print();

        env.execute();
    }
}
