package com.ngt.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;


/**
 * @author ngt
 * @create 2021-01-27 15:23
 */
public class FilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> nums = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 0);

        SingleOutputStreamOperator<Integer> filter = nums.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return (value & 1) == 0;
            }
        });

        filter.print();

        SingleOutputStreamOperator<Integer> filter1 = nums.filter(i -> (i & 1) == 0);
        filter1.print();

        // 同 map 可以调用 transform，更底层的实现方式
        SingleOutputStreamOperator<Integer> myFilter = nums.transform("MyFilter", TypeInformation.of(Integer.class), new MyStreamFilter());
        myFilter.print();

        env.execute();
    }

    public static class MyStreamFilter extends AbstractStreamOperator<Integer> implements OneInputStreamOperator<Integer, Integer> {
        @Override
        public void processElement(StreamRecord<Integer> element) throws Exception {
            Integer value = element.getValue();
            if ((value & 1) == 0) {
                output.collect(element.replace(value));
            }
        }
    }
}
