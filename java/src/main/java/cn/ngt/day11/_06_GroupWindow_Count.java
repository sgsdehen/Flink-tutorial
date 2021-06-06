package cn.ngt.day11;

import cn.ngt.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.rowInterval;

/**
 * Created on 2021-06-07 0:03.
 *
 * @author ngt
 */
public class _06_GroupWindow_Count {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.读取端口数据创建流并装换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("192.168.100.102", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        // 3.将流转换为表并指定处理时间
        Table table = tableEnv.fromDataStream(waterSensorDS,
                $("id"), $("ts"), $("vc"), $("pt").proctime());

        // 4.开滚动窗口计算WordCount
        Table resultTumble = table.window(Tumble.over(rowInterval(5L)).on($("pt")).as("cw"))
                .groupBy($("id"), $("cw"))
                .select($("id"), $("id").count());

        tableEnv.toAppendStream(resultTumble, Row.class).print();

        // 5.开滑动窗口计算WordCount
        Table resultSlide = table.window(Slide.over(rowInterval(10L)).every(rowInterval(5L)).
                on($("pt")).as("cw"))
                .groupBy($("id"), $("cw"))
                .select($("id"), $("id").count());
        tableEnv.toAppendStream(resultSlide, Row.class).print();

        env.execute();
    }
}
