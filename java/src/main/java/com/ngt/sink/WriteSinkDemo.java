package com.ngt.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ngt
 * @create 2021-01-25 22:45
 */
public class WriteSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> localhost = env.socketTextStream("192.168.31.8", 8888);

        /*
            已经过期，因为不能保证，建议使用 StreamingFileSink 替换
            注意并行度不为1的时候，不会生成文件，而是生成多个文件
         */
        localhost.writeAsText("data/1.txt");
        localhost.writeAsCsv("data/1.csv");

        env.execute();
    }
}
