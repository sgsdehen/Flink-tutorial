package cn.ngt.day10;

import cn.ngt.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author ngt on 2021-06-06 13:18
 * @version 1.0
 * 使用 SQL 查询未注册的表
 */
public class _09_SQL_Test {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// 2.读取端口数据转换为JavaBean
		SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("10.31.202.82", 9999)
				.map(data -> {
					String[] split = data.split(",");
					return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
				});

		// 3.将流转换成动态表
		Table table = tableEnv.fromDataStream(waterSensorDS);

		// 4.使用SQL查询未注册的表
		// 注意此处连接中的空格不能丢
		Table result = tableEnv.sqlQuery("select id, sum(vc) from " + table + " group by id");

		// 5.将表对象转换为流进行打印输出
		tableEnv.toRetractStream(result, Row.class).print();

		env.execute();
	}
}
