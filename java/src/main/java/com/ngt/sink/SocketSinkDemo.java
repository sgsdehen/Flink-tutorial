package com.ngt.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ngt
 * @create 2021-01-25 23:11
 */
public class SocketSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);
        lines.print();
        lines.writeToSocket("192.168.31.8", 9999, new SimpleStringSchema());
        env.execute();
    }
}
