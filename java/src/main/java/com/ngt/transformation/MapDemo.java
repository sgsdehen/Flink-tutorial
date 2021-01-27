package com.ngt.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * @author ngt
 * @create 2021-01-27 13:41
 */
public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

        // 1. 使用 map 匿名内部类
        SingleOutputStreamOperator<String> upperCase = lines.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toUpperCase();
            }
        });

        // 2. 使用 map 的 lambda表达式
        SingleOutputStreamOperator<String> upperCaseLambda = lines.map(String::toUpperCase);

        /*
           3. 手动调用 transform
            Params:
                operatorName – name of the operator, for logging purposes
                outTypeInfo – the output type of the operator
                operator – the object containing the transformation logic
         */
        SingleOutputStreamOperator<String> myMap = lines.transform("MyMap",
                TypeInformation.of(String.class), new StreamMap<>(String::toUpperCase));

        /*
            4. extends AbstractStreamOperator<OUT> implements OneInputStreamOperator<IN.OUT>
         */
        SingleOutputStreamOperator<String> myStreamMap = lines.transform("MyMap",
                TypeInformation.of(String.class), new MyStreamMap());


        System.out.println("mapParallelism: " + upperCase.getParallelism());

        upperCase.print();
        upperCaseLambda.print();
        myMap.print();
        myStreamMap.print();

        env.execute();
    }

    // 输出数据必须要继承 AbstractStreamOperator，同时如果不继承 AbstractStreamOperator 就需要实现超过 10 个方法
    public static class MyStreamMap extends AbstractStreamOperator<String> implements OneInputStreamOperator<String, String> {
        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            String value = element.getValue();
            String upperCase = value.toUpperCase();

            // 1. 创建一个 StreamRecord 的数据用于输出
            //StreamRecord<String> stringStreamRecord = new StreamRecord<>(upperCase);
            //output.collect(stringStreamRecord);

            // 2. 推荐使用replace方法，将要输出的数据替换原来的
            element.replace(upperCase);

            // 必须手动输出数据
            output.collect(element);
        }
    }
}
