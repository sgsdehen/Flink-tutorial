package com.ngt.partition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author ngt
 * @create 2021-01-28 4:56
 */
public class CustomPartitioning {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8181);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        DataStreamSource<String> words = env.socketTextStream("192.168.31.8", 8888);
        env.setParallelism(4);

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapDataStream = words.map(new RichMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
//                return value + " ：" + getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(value, getRuntimeContext().getIndexOfThisSubtask());
            }
        }).setParallelism(1);

        // 使用随机的方式将数据发送到下游
        DataStream<Tuple2<String, Integer>> partitionCustom = mapDataStream.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
                int res = 0;
                if ("spark".equals(key)) {
                    res = 1;
                } else if ("flink".equals(key)) {
                    res = 2;
                } else if ("hadoop".equals(key)) {
                    res = 3;
                }
                return res;
            }
        }, tp -> tp.f0);


        partitionCustom.addSink(new RichSinkFunction<Tuple2<String, Integer>>() {
            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                System.out.println(value.f0 + " -> " + getRuntimeContext().getIndexOfThisSubtask());
            }
        });

        env.execute();
    }
}

/*
    aa -> 0
    hadoop -> 3
    flink -> 2
    scala -> 0
    spark -> 1
    hadoop -> 3
    flink -> 2
    dfdf -> 0
 */