package com.ngt.transformation;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ngt
 * @create 2021-01-27 23:33
 */
public class UnionDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8181);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<String> lines1 = env.socketTextStream("192.168.31.8", 8888);
        DataStreamSource<String> lines2 = env.socketTextStream("192.168.31.8", 9999);

        // 两个 source 分支
        lines1.union(lines2).print();

        // 只有一个source分支，重复发送两次
        lines1.union(lines1).print();

        DataStreamSource<Integer> elements = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 0);

        // Union 要求流的数据类型必须相同
        // lines1.union(elements)
        env.execute();
    }
}
