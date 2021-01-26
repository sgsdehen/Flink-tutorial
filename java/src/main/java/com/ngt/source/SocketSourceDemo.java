package com.ngt.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ngt
 * @create 2021-01-25 12:53
 * SocketSource的并行度研究
 */
public class SocketSourceDemo {
    public static void main1(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // socketTextStream(String hostname, int port, String delimiter, long maxRetry)
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

        int parallelism = lines.getParallelism();
        System.out.println("SocketSourceParallelism:" + parallelism);

        // SocketSourceParallelism:1 socketTextStream是非并行的Source
        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });

        int parallelism1 = words.getParallelism();
        System.out.println("DataStreamParallelism:" + parallelism1);
        // DataStreamParallelism:8

        int parallelism2 = env.getParallelism();
        System.out.println("envParallelism:" + parallelism2);
        // envParallelism:8 本地默认的并行度就是处理器的逻辑核心数
        words.print();
        env.execute("SourceParallelism");



    }

    // 本地通过 WebUI 查看并行度及运行情况，需要先导入相关依赖
    public static void main(String[] args) throws Exception {
        // 自定义 WebUI 的接口
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8181);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> localhost = env.socketTextStream("192.168.31.8", 8888);

        // 并行度为1的有限数据量
        localhost.print();
        env.execute();
    }

}

/*
    SocketSourceParallelism:1
    DataStreamParallelism:8
 */