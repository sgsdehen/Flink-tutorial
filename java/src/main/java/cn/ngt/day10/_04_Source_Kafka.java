package cn.ngt.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;


import static org.apache.flink.table.api.Expressions.$;

/**
 * Created on 2021-06-05 21:01.
 *
 * @author ngt
 */
public class _04_Source_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("test")
                .startFromLatest()
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.100.102:9092")
                .property(ConsumerConfig.GROUP_ID_CONFIG, "bigdata"))
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("vc", DataTypes.INT()))
                // 支持 csv 和 json 两种格式
//                .withFormat(new Csv())
                .withFormat(new Json())
                .createTemporaryTable("sensor");

        //3.使用连接器创建表
        Table sensor = tableEnv.from("sensor");

        //4.查询数据
        Table resultTable = sensor.groupBy($("id"))
                .select($("id"), $("id").count());

        //6.将表转换为流进行输出
        tableEnv.toRetractStream(resultTable, Row.class).print();

        //7.执行任务
        env.execute();
    }
}
