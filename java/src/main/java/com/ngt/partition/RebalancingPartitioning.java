package com.ngt.partition;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author ngt
 * @create 2021-01-28 3:04
 */
public class RebalancingPartitioning {
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
        DataStream<String> rebalance = mapDataStream.rebalance();

        rebalance.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                System.out.println(value + " -> " + getRuntimeContext().getIndexOfThisSubtask());
            }
        });

        env.execute();

    }
}

/*
    使用轮询的方式发送到下游，注意是上游的节点分别轮询
    aaaa ： 0 -> 1
    aaaa ： 1 -> 0
    gggg ： 0 -> 2
    cccc ： 1 -> 1
    bbbb ： 0 -> 3
    hhhh ： 1 -> 2
    wwww ： 0 -> 0
    nnnn ： 1 -> 3
 */