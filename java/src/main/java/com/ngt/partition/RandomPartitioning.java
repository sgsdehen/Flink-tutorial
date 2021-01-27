package com.ngt.partition;

import org.apache.flink.api.common.functions.RichMapFunction;
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
 * @create 2021-01-28 4:39
 */
public class RandomPartitioning {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8181);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        DataStreamSource<String> words = env.socketTextStream("192.168.31.8", 8888);
        env.setParallelism(4);

        SingleOutputStreamOperator<String> mapDataStream = words.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value + " ：" + getRuntimeContext().getIndexOfThisSubtask();
            }
        }).setParallelism(1);

        // 使用随机的方式将数据发送到下游
        DataStream<String> random = mapDataStream.shuffle();

        random.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, SinkFunction.Context context) throws Exception {
                System.out.println(value + " -> " + getRuntimeContext().getIndexOfThisSubtask());
            }
        });

        env.execute();
    }
}

/*
    随机发送到下游
    aaaa ：0 -> 2
    aaaa ：0 -> 3
    bbbb ：0 -> 3
    dddd ：0 -> 2
    eeee ：0 -> 2
    aaaa ：0 -> 0
    cccc ：0 -> 1
    oooo ：0 -> 1

 */