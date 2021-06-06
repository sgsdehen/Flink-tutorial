package cn.ngt.day11;

import cn.ngt.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * Created on 2021-06-06 22:28.
 *
 * @author ngt
 */
public class _05_GroupWindow_ProcessTime {
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
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());

        // 4.开滚动窗口计算WordCount
//        Table resultTumble = table.window(Tumble.over(lit(5).seconds()).on($("pt")).as("tw"))
//                .groupBy($("id"), $("tw"))
//                .select($("id"), $("id").count());
//
//        tableEnv.toAppendStream(resultTumble, Row.class).print();


        // 5.开滑动窗口计算 WordCount  每个输入属于3个窗口
//        Table resultSlide = table.window(Slide.over(lit(6).seconds())
//                .every(lit(2).seconds())
//                .on($("pt")).as("sw"))
//                .groupBy($("id"), $("sw"))
//                .select($("id"), $("id").count());
//
//        tableEnv.toAppendStream(resultSlide, Row.class).print();

        // 6.开会话窗口计算WordCount
        Table resultSession = table.window(Session.withGap(lit(5).seconds()).on($("pt")).as("sw"))
                .groupBy($("id"), $("sw"))
                .select($("id"), $("id").count());

        tableEnv.toAppendStream(resultSession, Row.class).print();
        env.execute();
    }
}
