package com.ngt.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author ngt
 * @create 2021-01-25 17:25
 */
public class PrintSinkDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8181);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> localhost = env.socketTextStream("192.168.31.8", 8888);

        localhost.addSink(new MyPrintSink()).name("MyPrintSink");
        env.execute();
    }

    // 自定义实现 print 打印方法
    public static class MyPrintSink extends RichSinkFunction<String> {
        int indexOfThisSubtask;

        @Override
        public void open(Configuration parameters) throws Exception {
            indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            super.open(parameters);
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            System.out.println(indexOfThisSubtask + 1 + "> " + value);
        }
    }
}
/*
轮询的方式打印
5> 1
6> 1
7> 1
8> 1
1> 1
2> 1
3> 1
4> 1
 */