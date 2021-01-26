package com.ngt.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author ngt
 * @create 2021-01-25 16:36
 */
public class CustomSourceDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8181);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(4);

        DataStreamSource<String> source = env.addSource(new MySource());
        source.print();
        env.execute();
    }

    // 可以获取运行时上下文
    public static class MySource extends RichParallelSourceFunction<String> {

        // 1. 调用构造方法
        // 2. 调用 open 方法
        // 3. 调用 run 方法
        // 4. 调用 cancel 方法
        // 5. 调用 close 方法

        private boolean flag = true; //定义一个flag标标志
        private int indexOfThisSubtask;

        @Override
        public void open(Configuration parameters) throws Exception {
            indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("subTask" + indexOfThisSubtask + " open method invoked");
            super.open(parameters);
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("subTask" + indexOfThisSubtask + " run method invoked");
            while (flag) {
                TimeUnit.SECONDS.sleep(1); //为避免太快，睡眠1秒
                ctx.collect("subTask" + indexOfThisSubtask + " " +
                        UUID.randomUUID().toString()); //将数据通过SourceContext收集起来
            }
        }

        // 该方法通过 WebUI 中侧 cancel 按钮触发
        @Override
        public void cancel() {
            indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("subTask" + indexOfThisSubtask + " cancel method invoked");
            flag = false;
        }

        @Override
        public void close() throws Exception {
            indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("subTask " + indexOfThisSubtask + " close method invoked");
            super.close();
        }
    }
}

/*
    subTask1 open method invoked
    subTask0 open method invoked
    subTask3 open method invoked
    subTask2 open method invoked
    subTask1 run method invoked
    subTask0 run method invoked
    subTask3 run method invoked
    subTask2 run method invoked
    1> subTask0 37d8335f-c007-44ec-ba05-147a8657f9e7
    3> subTask2 4730ec43-f0d4-4cb9-a49d-2d0578774d44
    2> subTask1 cc568cf8-b1d4-4ee1-9f81-2128101cf7a2
    4> subTask3 10acf059-aba9-4bc4-bd75-247ff2ae5972
    subTask0 cancel method invoked
    subTask1 cancel method invoked
    subTask2 cancel method invoked
    subTask3 cancel method invoked
    subTask0 close method invoked
    subTask1 close method invoked
    subTask2 close method invoked
    subTask3 close method invoked
 */