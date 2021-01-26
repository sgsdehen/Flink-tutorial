package com.ngt.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

import java.util.Arrays;

/**
 * @author ngt
 * @create 2021-01-25 13:27
 * 单并行的source 直接实现SourceFunction
 * 多并行的source 实现了RichParallelSourceFunction或 ParallelSourceFunction
 */
public class CollectionSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println("StreamExecutionEnvironmentParallelism: "+env.getParallelism());


        // 1. fromElements 非并行有限数据的Source
        DataStreamSource<String> elements = env.fromElements("flink", "hadoop", "spark");
        System.out.println("fromElementsParallelism: " +  elements.getParallelism());


        // 2. fromCollection 非并行有限数据的Source，接收Collection类型的数据
        DataStreamSource<String> collection = env.fromCollection(Arrays.asList("flink", "hadoop", "spark"));
        System.out.println("collectionParallelism: " +  collection.getParallelism());


        // 3. fromParallelCollection 并行的Source，可以设置并行度
        // 第一个是继承SplittableIterator的实现类的迭代器，第二个是迭代器中数据的类型
        DataStreamSource<Long> parallelCollection =
                env.fromParallelCollection(new NumberSequenceIterator(1L, 100L), Long.class).setParallelism(4);
        System.out.println("parallelCollectionParallelism: " +  parallelCollection.getParallelism());


        // 4. generateSequence 并行的Source该方法需要传入两个long类型的参数，第一个是起始值，第二个是结束值
        DataStreamSource<Long> generateSequence = env.generateSequence(1, 100).setParallelism(4);
        System.out.println("generateSequenceParallelism: " +  generateSequence.getParallelism());

        // 5. fromSequence, 与 generateSequence 相同用于替换其
        DataStreamSource<Long> fromSequence = env.fromSequence(1, 100).setParallelism(4);
        System.out.println("fromSequenceParallelism: " +  fromSequence.getParallelism());
        fromSequence.print();
        env.execute("CollectionSourceDemo");
    }
}

/*
    StreamExecutionEnvironmentParallelism: 8
    fromElementsParallelism: 1
    collectionParallelism: 1
    parallelCollectionParallelism: 4
    generateSequenceParallelism: 4
    fromSequenceParallelism: 4
 */