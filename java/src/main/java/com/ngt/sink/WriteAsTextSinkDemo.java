package com.ngt.sink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.File;
import java.util.Arrays;

/**
 * @author ngt
 * @create 2021-01-25 22:45
 */
public class WriteAsTextSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> localhost = env.socketTextStream("192.168.31.8", 8888);

        /*
            已经过期，因为不能保证，建议使用 StreamingFileSink 替换
            注意并行度不为1的时候，会生成多个分片文件
         */
        env.setParallelism(1);

        String textPath = "data/1.txt";
        File textfile = new File(textPath);
        if (textfile.exists()) {
            textfile.delete();
        }
        localhost.print();
        localhost.writeAsText(textPath);

        env.execute();
    }
}
