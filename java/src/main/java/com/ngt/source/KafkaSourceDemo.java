package com.ngt.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author ngt
 * @create 2021-01-25 15:15
 */
public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        //指定Kafka的Broker地址
        props.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        //指定组ID
        props.setProperty("group.id", "gwc10");
        //如果没有记录偏移量，第一次从最开始消费
        props.setProperty("auto.offset.reset", "earliest");
        //kafka的消费者是否自动提交偏移量
        //props.setProperty("enable.auto.commit", "false");


        //KafkaSource
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
                "wc10",
                new SimpleStringSchema(),
                props);

        //Source
        DataStream<String> lines = env.addSource(kafkaSource);

        //Sink
        lines.print();

        env.execute("KafkaSource");
    }
}
