package com.ngt.function;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * @author ngt
 * @create 2021-02-01 0:16
 * 使用测流输出数据中的偶数
 * https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/stream/side_output.html
 */
public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

        OutputTag<String> oddOutputTag = new OutputTag<>("odd") { // 奇数
        };
        OutputTag<String> evenOutputTag = new OutputTag<>("even") {// 偶数
        };
        OutputTag<String> nanOutputTag = new OutputTag<>("nan") { // 非数字
        };

        SingleOutputStreamOperator<String> mainStream = lines.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                try {
                    int i = Integer.parseInt(value);
                    if ((i & 1) == 1) {
                        ctx.output(oddOutputTag, value);
                    } else {
                        ctx.output(evenOutputTag, value);
                    }

                } catch (NumberFormatException e) {
                    ctx.output(nanOutputTag, value);
                }
                // 主流中输出全部的数据，否则主流没有输出
                out.collect(value);
            }
        });

        mainStream.getSideOutput(evenOutputTag).print("even");
        mainStream.getSideOutput(oddOutputTag).print("odd");
        mainStream.print("main");
        env.execute();
    }
}
