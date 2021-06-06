package cn.ngt.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author ngt on 2021-06-06 14:25
 * @version 1.0
 */
public class _11_SQL_KafkaToKafka {
	public static void main(String[] args) {
		// 1.获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// 2.注册SourceTable
		tableEnv.executeSql("create table source_sensor (id string, ts bigint, vc int) with ("
				+ "  'connector' = 'kafka',"
				+ "  'topic' = 'topic_source',"
				+ "  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',"
				+ "  'properties.group.id' = 'testGroup',"
				+ "  'scan.startup.mode' = 'latest-offset',"
				+ "  'format' = 'csv'"
				+ ")");

		// 3. 注册SinkTable: sink_sensor
		tableEnv.executeSql("create table sink_sensor(id string, ts bigint, vc int) with("
				+ "'connector' = 'kafka',"
				+ "'topic' = 'topic_sink',"
				+ "'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',"
				+ "'format' = 'json'"
				+ ")");

		//4.执行查询插入数据
		tableEnv.executeSql("insert into sink_sensor select * from source_sensor where id = 'ws_001'");

		// 不需要 env.execute()
	}
}
