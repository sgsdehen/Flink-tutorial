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
public class WriteAsCsvSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /*
            已经过期，因为不能保证，建议使用 StreamingFileSink 替换
            注意并行度不为1的时候，会生成多个分片文件
         */

        // 注意 csv 只能输出元组类型
        DataStreamSource<String> words = env.fromElements("spark", "flink", "hadoop", "spark", "flink", "flink");
        SingleOutputStreamOperator<String> word = words.flatMap((String line, Collector<String> out) ->
                Arrays.stream(line.split(" ")).forEach(out::collect)).returns(Types.STRING);


        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne =
                word.map(w -> Tuple2.of(w, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));

        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = wordAndOne.keyBy(f -> f.f0).sum(1);

        String csvPath = "data/1.csv";
        File csvfile = new File(csvPath);
        if (csvfile.exists()) {
            csvfile.delete();
        }
        summed.print();
        summed.writeAsCsv(csvPath);
        env.execute();
    }
}
