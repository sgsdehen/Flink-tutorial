package com.ngt.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import java.util.Properties;

/**
 * @author ngt
 * @create 2021-01-25 18:53
 */
public class KafkaSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // socketTextStream(String hostname, int port, String delimiter, long maxRetry)
        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("192.168.100.102:9092",
                "sinktest", new SimpleStringSchema());

        lines.addSink(kafkaProducer);

        env.execute();
    }
}

//    bin/kafka-console-consumer.sh --bootstrap-server hadoop102:9092 --topic sinktest
//    bin/kafka-console-producer.sh --broker-list hadoop102:9092 --topic sensor0