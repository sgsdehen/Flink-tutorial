package cn.ngt.day10;

import cn.ngt.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Created on 2021-06-05 16:48.
 *
 * @author ngt
 */
public class _01_StreamToTable {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.读取端口数据创建流并装换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("192.168.100.102", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        // 3.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 4.将流转换成动态表
        Table sensorTable = tableEnv.fromDataStream(waterSensorDS);

        // 5.使用TableAPI过滤数据
        Table selectTable = sensorTable
                .where($("id").isEqual("ws_001"))
                .select($("id"), $("ts"), $("vc"));

        // 已过时API
        Table selectTable1 = selectTable
                .where("id='ws_001'")
                .select("id,ts,vc");

        // 6. 将selectTable转换为追加流流进行输出
        DataStream<Row> rowDataStream1 = tableEnv.toAppendStream(selectTable, Row.class);
        //rowDataStream1.print(); // ws_001,1577844001,45

        DataStream<Tuple2<Boolean, Row>> rowDataStream2 = tableEnv.toRetractStream(selectTable, Row.class);
        rowDataStream2.print();

        //7.执行任务
        env.execute();
    }
}

/*
使用
 */
