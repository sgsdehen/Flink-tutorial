package cn.ngt.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author ngt on 2021-06-06 14:46
 * @version 1.0
 */
public class _12_SQL_MySQL {
	public static void main(String[] args) {
		// 1.获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// 2.注册SourceTable
		tableEnv.executeSql("create table source_sensor (id string, ts bigint, vc int) with("
				+ "'connector' = 'kafka',"
				+ "'topic' = 'topic_source',"
				+ "'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',"
				+ "'properties.group.id' = 'testGroup',"
				+ "'scan.startup.mode' = 'latest-offset',"
				+ "'format' = 'csv'"
				+ ")");

		// 3.注册SinkTable：MySQL,表并不会自动创建
		tableEnv.executeSql("create table sink_sensor (id string, ts bigint, vc int) with("
				+ "'connector' = 'jdbc',"
				+ "'url' = 'jdbc:mysql://10.31.4.199:3306/test',"
				+ "'table-name' = 'sink_table',"
				+ "'username' = 'root',"
				+ "'password' = '123456'"
				+ ")");

		tableEnv.executeSql("insert into sink_sensor select * from source_sensor");
	}
}
