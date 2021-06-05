package cn.ngt.day10;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;


import static org.apache.flink.table.api.Expressions.$;

/**
 * Created on 2021-06-05 20:13.
 *
 * @author ngt
 */
public class _03_Source_File {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.使用Connect方式读取文本数据
        tableEnv.connect(new FileSystem().path("data/sensor.txt"))
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("vc", DataTypes.DOUBLE())) //注意没有 Long 使用 BigInt
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n")) // 注意此处要使用单引号
                .createTemporaryTable("sensor");

        //3.将连接器应用,转换为表
        Table sensor = tableEnv.from("sensor");

        //4.查询
        Table resultTable = sensor.groupBy($("id"))
                .select($("id"), $("id").count().as("ct"));

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(resultTable, Row.class);

        tuple2DataStream.print();

        env.execute();
    }
}
