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
import static org.apache.flink.table.api.Expressions.e;

/**
 * Created on 2021-06-05 19:56.
 *
 * @author ngt
 */
public class _02_StreamToTable_Agg {
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

        // 4.将数据转换为动态表
        Table sensorTable = tableEnv.fromDataStream(waterSensorDS);

        // 5.使用TableAPI 实现 select id,sum(vc) from sensor where vc>=20 group by id;
        Table selectTable = sensorTable
                .where($("vc").isGreaterOrEqual(20))
                .groupBy($("id"))
                .aggregate($("vc").sum().as("sum_vc")) //
                .select($("id"), $("sum_vc"));

        // 6.将selectTable转换为流进行输出
        DataStream<Tuple2<Boolean, Row>> rowDataStream = tableEnv.toRetractStream(selectTable, Row.class);
        rowDataStream.print();

        env.execute();
    }
}

/*
撤回流返回的是一个2元组类型，其中第一个参数标识是否撤回
输入：ws_001,1577844001,45
输出：(true,ws_001,45)

// 将 ws_001,45 标记为撤回，重新写入  ws_001,90
输入：ws_001,1577844001,45
输出：(true,ws_001,45)
     (true,ws_001,90)


 */