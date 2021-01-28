package com.ngt.window;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author ngt
 * @create 2021-01-28 18:15
 * apply 不使用增量操作，将窗口内的数据先存起来，放在WindowState中
 */
public class CountWindowAllApplyDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8181);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);
        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        // 不分组，将整体当成一个组。并行度为1
        AllWindowedStream<Integer, GlobalWindow> window = nums.countWindowAll(5);

        window.apply(new AllWindowFunction<Integer, Integer, GlobalWindow>() {

            /**
             *
             * @param window
             * @param values  输入的数据
             * @param out     输出的数据
             * @throws Exception
             */
            @Override
            public void apply(GlobalWindow window, Iterable<Integer> values, Collector<Integer> out) throws Exception {
                ArrayList<Integer> list = new ArrayList<>();
                for (Integer value : values) {
                    list.add(value);
                }
                list.sort(Integer::compareTo); // 升序
//                list.sort(Comparator.reverseOrder()); // 降序
                for (Integer i : list) {
                    out.collect(i);
                }
            }
        }).print().setParallelism(1); // 此处必须将print的并行度设置为1 才能按照顺序输出
        env.execute();
    }
}
