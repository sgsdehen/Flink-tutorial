package com.ngt.demo;

import com.ngt.sink.RedisSinkDemo;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author ngt
 * @create 2021-02-01 21:00
 */
public class KafkaToRedisWordCount {
    public static void main(String[] args) throws Exception {
        // 参数工具 --topic wc --groupId g1
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8181);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.enableCheckpointing(3000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5000)));
        // 状态存储的后端
        System.setProperty("HADOOP_USER_NAME", "ngt");
        env.setStateBackend(new FsStateBackend("hdfs://192.168.100.102:9000/tmp"));

        Properties props = new Properties();
        //指定Kafka的Broker地址
        props.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        //指定组ID
        props.setProperty("group.id", parameterTool.get("groupId"));
        //如果没有记录偏移量，第一次从最开始消费
        props.setProperty("auto.offset.reset", "earliest");
        //kafka的消费者不自动提交偏移量
        props.setProperty("enable.auto.commit", "true");

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
                parameterTool.getRequired("topic"), // 必须要此参数
                new SimpleStringSchema(),
                props);

        DataStreamSource<String> lines = env.addSource(kafkaSource);

        SingleOutputStreamOperator<String> word = lines.flatMap((String line, Collector<String> out) ->
                Arrays.stream(line.split(" ")).forEach(out::collect)).returns(Types.STRING);


        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne =
                word.map(w -> Tuple2.of(w, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));

        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = wordAndOne.keyBy(f -> f.f0).sum(1);

        summed.print();
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.31.8").setPort(6379).build();
        summed.addSink(new RedisSink<Tuple2<String, Integer>>(conf, new RedisSinkDemo.MyRidesSink()));
        env.execute();
    }

    private static class MyRidesSink implements RedisMapper<Tuple2<String, Integer>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "wordconut");
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1.toString();
        }
    }
}

//    bin/kafka-console-consumer.sh --bootstrap-server hadoop102:9092 --topic wc
//    bin/kafka-console-producer.sh --broker-list hadoop102:9092 --topic wc

/*
core-site.xml 配置文件中添加配置并分发

<property>
    <name>fs.default.name</name>
    <value>hdfs://hadoop102:9000</value>
</property>

 */