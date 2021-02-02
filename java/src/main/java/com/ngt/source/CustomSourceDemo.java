package com.ngt.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * @author ngt
 * @create 2021-01-25 15:35
 */
public class CustomSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> source1 = env.addSource(new MySource1());
        System.out.println("SourceFunction的并行度：" + source1.getParallelism());
        // SourceFunction的并行度：1
        source1.print();


        DataStreamSource<String> source2 = env.addSource(new MySource2());
        System.out.println("ParallelSourceFunction的并行度：" + source2.getParallelism());
        // ParallelSourceFunction的并行度：4

        source2.setParallelism(2);
        System.out.println("调整后ParallelSourceFunction的并行度：" + source2.getParallelism());
        // 调整后ParallelSourceFunction的并行度: 2
        source2.print();

        env.execute();

    }

    // 实现 SourceFunction 接口的Source是非并行，有限的数据量
    public static class MySource1 implements SourceFunction<String> {
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            List<String> words = Arrays.asList("a", "b", "c", "d", "e");
            for (String word : words) {
                ctx.collect(word);
            }
        }

        // WubUI中的 Cancel Job按钮触发该函数
        @Override
        public void cancel() {

        }
    }

    // 实现 SourceFunction 接口Source是多并行的，有限的数据量
    public static class MySource2 implements ParallelSourceFunction<String> {
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            List<String> words = Arrays.asList("a", "b", "c", "d", "e");
            for (String word : words) {
                ctx.collect(word);
            }
        }

        // WubUI中的 Cancel Job按钮触发该函数
        @Override
        public void cancel() {

        }
    }


}
