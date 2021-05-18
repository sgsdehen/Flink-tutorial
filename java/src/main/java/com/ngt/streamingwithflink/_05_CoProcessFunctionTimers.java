package com.ngt.streamingwithflink;

import com.ngt.streamingwithflink.util.SensorReading;
import com.ngt.streamingwithflink.util.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author ngt
 * @create 2021-05-18 9:05
 */
public class _05_CoProcessFunctionTimers {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setAutoWatermarkInterval(1000L);

		DataStreamSource<Tuple2<String, Long>> filterSwitches = env.fromCollection(Arrays.asList(
				Tuple2.of("sensor_2", 5 * 1000L), Tuple2.of("sensor_7", 10 * 1000L)));

		DataStreamSource<SensorReading> readings = env.addSource(new SensorSource());

		readings.connect(filterSwitches)
				.keyBy(data -> data.id, data -> data.f0)
				.process(new ReadingFilter())
				.print();

		env.execute();
	}

	static class ReadingFilter extends CoProcessFunction<SensorReading, Tuple2<String, Long>, SensorReading> {

		private transient ValueState<Boolean> forwardingEnabled;
		private transient ValueState<Long> disableTimer;

		@Override
		public void open(Configuration parameters) throws Exception {
			forwardingEnabled = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("filterSwitch", Boolean.class));
			disableTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));
		}

		@Override
		public void processElement1(SensorReading value,
									Context ctx,
									Collector<SensorReading> out) throws Exception {
			// Java版本中要注意为空的情况
			Boolean forward = forwardingEnabled.value();
			if (forward != null && forward) {
				out.collect(value);
			}
		}

		@Override
		public void processElement2(Tuple2<String, Long> value,
									Context ctx,
									Collector<SensorReading> out) throws Exception {
			forwardingEnabled.update(true);
			Long curTimerTimestamp = disableTimer.value();
			long timerTimestamp = ctx.timerService().currentProcessingTime() + value.f1;
			if (curTimerTimestamp == null || timerTimestamp > curTimerTimestamp) {
				if (curTimerTimestamp != null) {
					ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp);
				}
				ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
				disableTimer.update(timerTimestamp);
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<SensorReading> out) throws Exception {
			forwardingEnabled.clear();
			disableTimer.clear();
		}
	}
}
