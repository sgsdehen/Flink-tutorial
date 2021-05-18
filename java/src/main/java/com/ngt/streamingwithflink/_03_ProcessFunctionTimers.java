package com.ngt.streamingwithflink;

import com.ngt.streamingwithflink.util.SensorReading;
import com.ngt.streamingwithflink.util.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @author ngt
 * @create 2021-05-17 22:29
 */
public class _03_ProcessFunctionTimers {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new SensorSource())
                .keyBy(data -> data.id)
                .process(new TempIncreaseAlertFunction())
                .print();

        env.execute();
    }

    private static class TempIncreaseAlertFunction extends KeyedProcessFunction<String, SensorReading, String> {

        // 存储最近一次的温度值
        private transient ValueState<Double> lastTemp;
        // 存储当前活动计时器的时间戳
        private transient ValueState<Long> currentTimer;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", Double.class));
            currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("currentTimer", Long.class));
        }

        @Override
        public void processElement(SensorReading value,
                                   Context ctx,
                                   Collector<String> out) throws Exception {
            Double prevTemp = lastTemp.value();
            lastTemp.update(value.temperature);

            Long curTimerTimestamap = currentTimer.value();

            if (prevTemp == 0 || prevTemp > value.temperature) {
                // 温度下降删除定时器
                ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamap);
                currentTimer.clear();
            } else {
                // 温度升高且没有设置计时器
                ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1000L);
                currentTimer.update(ctx.timerService().currentProcessingTime() + 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp,
                            OnTimerContext ctx,
                            Collector<String> out) throws Exception {
            out.collect("Sensor " + ctx.getCurrentKey() + "温度持续上升1秒钟");
        }
    }
}


