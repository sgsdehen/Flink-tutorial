package com.ngt.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

/**
 * @author ngt
 * @create 2021-01-27 14:21
 */
public class FlatMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);



        // 1. 使用 flatMap 拆分并过滤
        SingleOutputStreamOperator<String> mapOut = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    if (!"error".equals(word)) {
                        out.collect(word);
                    }
                }
            }
        });
//        mapOut.print();

        // 使用 lambda 表达式，必须要调用returns方法指定返回的数据的类型。不然Flink无法自动推断出返回的数据类型，会出现异常
        SingleOutputStreamOperator<String> mapOut1  = lines.flatMap((String words, Collector<String> out) -> {
            String[] line = words.split(" ");
            for (String word : line) {
                if (!"error".equals(word)) {
                    out.collect(word);
                }
            }
        }).returns(Types.STRING);
        mapOut1.print();

        // 2. extends AbstractStreamOperator<OUT> implements OneInputStreamOperator<IN.OUT> 实现
        SingleOutputStreamOperator<String> myFlatMap = lines.transform("MyFlatMap",
                TypeInformation.of(String.class), new MyFlatMap());

        myFlatMap.print();
        env.execute();
    }

    public static class MyFlatMap extends AbstractStreamOperator<String> implements OneInputStreamOperator<String, String> {
        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            String line = element.getValue();
            String[] words = line.split(" ");
            for (String word : words) {
                if (!"error".equals(word)) {
                    // 注意此处要使用 replace 方法替换
                    output.collect(element.replace(word));
                }
            }
        }
    }
}
