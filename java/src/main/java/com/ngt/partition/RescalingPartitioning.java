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
 * @create 2021-01-28 4:49
 */
public class RescalingPartitioning {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8181);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        DataStreamSource<String> words = env.socketTextStream("192.168.31.8", 8888);
        env.setParallelism(4);

        SingleOutputStreamOperator<String> mapDataStream = words.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value + " ： " + getRuntimeContext().getIndexOfThisSubtask();
            }
        }).setParallelism(2);

        // 使用轮询的方式将数据发送到下游
        DataStream<String> rescale = mapDataStream.rescale();

        rescale.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, SinkFunction.Context context) throws Exception {
                System.out.println(value + " -> " + getRuntimeContext().getIndexOfThisSubtask());
            }
        });

        env.execute();
    }
}
/*
    不会发送到下游所有的Task，而是对应发送
    0 -> 0,1  1 -> 2,3
    aaaa ： 1 -> 2
    aaaa ： 0 -> 0
    gggg ： 1 -> 3
    cccc ： 0 -> 1
    bbbb ： 1 -> 2
    hhhh ： 0 -> 0
    wwww ： 1 -> 3
    nnnn ： 0 -> 1
 */