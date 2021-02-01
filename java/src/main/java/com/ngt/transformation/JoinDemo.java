package com.ngt.transformation;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author ngt
 * @create 2021-01-27 23:46
 * 窗口联接联接共享一个公共键并位于同一窗口中的两个流的元素。
 * 可以使用窗口分配器定义这些窗口，并在两个流的元素上计算这些窗口。
 */
public class JoinDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8181);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // DataStream1
        DataStreamSource<String> lines1 = env.socketTextStream("192.168.31.8", 8888);

        SingleOutputStreamOperator<String> word1 = lines1.flatMap((String line, Collector<String> out) ->
                Arrays.stream(line.split(" ")).forEach(out::collect)).returns(Types.STRING);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne1 =
                word1.map(w -> Tuple2.of(w, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));

        // DataStream2
        DataStreamSource<String> lines2 = env.socketTextStream("192.168.31.8", 9999);

        SingleOutputStreamOperator<String> word2 = lines2.flatMap((String line, Collector<String> out) ->
                Arrays.stream(line.split(" ")).forEach(out::collect)).returns(Types.STRING);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne2 =
                lines2.map(w -> Tuple2.of(w, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));

        KeyedStream<Tuple2<String, Integer>, String> keyBy1 = wordAndOne1.keyBy(data -> data.f0);
        KeyedStream<Tuple2<String, Integer>, String> keyBy2 = wordAndOne2.keyBy(data -> data.f0);

        keyBy1.join(keyBy2).where(data -> data.f0).equalTo(data -> data.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
                        return Tuple2.of(first.f0, first.f1 + second.f1);
                    }
                }).print();

        env.execute();
    }
}
