package com.ngt.sink;

import com.ngt.util.HBaseUtil;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.util.Arrays;

/**
 * @author ngt
 * @create 2021-02-02 19:07
 */
public class HbaseSinkDemo {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(8);

        DataStreamSource<String> lines = env.socketTextStream("192.168.31.8", 8888);

        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, "192.168.100.102,192.168.100.103,192.168.100.104");
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);

        HBaseUtil hBaseUtil = new HBaseUtil();

        lines.addSink(hBaseUtil.new HBaseWriter(config, "StationTraffic", "traffic"));
    }

}

