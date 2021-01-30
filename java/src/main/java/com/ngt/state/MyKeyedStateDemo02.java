package com.ngt.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author ngt
 * @create 2021-01-30 23:11
 * 自己在每个keyed之后的subTask中定义一个hashMap保存中间结果
 * 定期将hashmap中的数据持久化到本地，
 * 并且subtask出现异常，在open方法中可以读取本地文件中的数据用于恢复历史状态
 */
public class MyKeyedStateDemo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

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


        KeyedStream<Tuple2<String, Integer>, String> keyed = words.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0;
            }
        });

        // 不调用 sum，因为其会存在状态,自己实现状态存储， 使用 RichMapFunction 便于获取运行时上下文
        keyed.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            // 用于存储中间结果
            private HashMap<String, Integer> counter;

            // 使用open方法，初始化 hashmap 或者恢复历史状态
            @Override
            public void open(Configuration parameters) throws Exception {
                // 为了将不同的subTask写入对应的文件中
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                File ckFile = new File("data/myck/" + indexOfThisSubtask);

                if (ckFile.exists()) { // 存在就读取
                    FileInputStream fileInputStream = new FileInputStream(ckFile);
                    ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                    counter = (HashMap<String, Integer>) objectInputStream.readObject();
                } else { // 不存在就创建
                    counter = new HashMap<>();
                }

                // 定时保存策略，使用新的线程定期将内存的数据写到磁盘
                new Thread(() -> {
                    while (true) {
                        try {
                            TimeUnit.SECONDS.sleep(5);
                            if (!ckFile.exists()) {
                                ckFile.createNewFile();
                            }
                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(ckFile));
                            objectOutputStream.writeObject(counter);
                            objectOutputStream.flush();
                            objectOutputStream.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }).start();
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> input) throws Exception {
                String word = input.f0;
                Integer count = input.f1;
                Integer historyCount = counter.get(word);
                if (historyCount == null) {
                    historyCount = 0;
                }
                int sum = historyCount + count;
                // 更新 map 中的数据
                counter.put(word, sum);
                // 输出结果
                return Tuple2.of(word, sum);
            }
        }).print();

        env.execute();
    }
}
