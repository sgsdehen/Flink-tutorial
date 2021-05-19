package com.ngt.streamingwithflink;

import com.ngt.streamingwithflink.util.SensorReading;
import com.ngt.streamingwithflink.util.SensorSource;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;

/**
 * @author ngt
 * @create 2021-05-18 11:32
 */
public class _07_CustomWindows {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 1. ReduceFunction 使用lambda表达式
		env.addSource(new SensorSource())
				.assignTimestampsAndWatermarks(WatermarkStrategy
						.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(1))
						.withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
							@Override
							public long extractTimestamp(SensorReading element, long recordTimestamp) {
								return element.timestamp;
							}
						}))
				.keyBy(data ->data.id)
				// 窗口分配器
				.window(new ThirtySecondsWindows())
				// 可选项：指定分类器
				.trigger(new OneSecondIntervalTrigger())
				// 指定窗口函数
				.process(new CountFunction())
				.print();

		env.execute();
	}

	// 按照每30秒滚动窗口进行分组的自定义窗口
	static class ThirtySecondsWindows extends WindowAssigner<Object, TimeWindow> {
		private static final long windowSize = 30 * 1000L;

		@Override
		public Collection<TimeWindow> assignWindows(Object element,
													long timestamp,
													WindowAssignerContext context) {
			long startTime = timestamp - (timestamp % windowSize);
			long endTime = startTime + windowSize;
			return Collections.singletonList(new TimeWindow(startTime, endTime));
		}

		@Override
		public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
			return EventTimeTrigger.create();
		}

		@Override
		public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
			return new TimeWindow.Serializer();
		}

		@Override
		public boolean isEventTime() {
			return true;
		}
	}

	static class OneSecondIntervalTrigger extends Trigger<SensorReading, TimeWindow> {
		@Override
		public TriggerResult onElement(SensorReading element,
									   long timestamp,
									   TimeWindow window,
									   TriggerContext ctx) throws Exception {
			// 获取一个作用域为触发器键值和当前窗口的状态对象
			ValueState<Boolean> firstSeen =
					ctx.getPartitionedState(new ValueStateDescriptor<>("firstSeen", Types.BOOLEAN));
			if (firstSeen.value() == null) {
				long t = ctx.getCurrentWatermark() + (1000 - (ctx.getCurrentWatermark() % 1000));
				ctx.registerEventTimeTimer(t);

				ctx.registerEventTimeTimer(window.getEnd());
				firstSeen.update(true);
			}
			return TriggerResult.CONTINUE;
		}

		@Override
		public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
			return TriggerResult.CONTINUE; // 不使用处理时间
		}

		@Override
		public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
			if (time == window.getEnd()) {
				return TriggerResult.FIRE_AND_PURGE; // 先计算再删除
			} else {
				long t = ctx.getCurrentWatermark() + (1000 - (ctx.getCurrentWatermark() % 1000));
				if (t < window.getEnd()) {
					ctx.registerEventTimeTimer(t);
				}
				return TriggerResult.FIRE;
			}
		}

		@Override
		public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
			ValueState<Boolean> firstSeen =
					ctx.getPartitionedState(new ValueStateDescriptor<>("firstSeen", Types.BOOLEAN));
			firstSeen.clear();
		}
	}

	static class CountFunction extends ProcessWindowFunction<SensorReading, Tuple4<String, Long, Long, Integer>, String, TimeWindow> {
		@Override
		public void process(String s,
							Context context,
							Iterable<SensorReading> elements,
							Collector<Tuple4<String, Long, Long, Integer>> out) throws Exception {
			int cnt = 0;
			for (SensorReading element : elements) {
				cnt++;
			}
			long evalTime = context.currentWatermark();
			out.collect(Tuple4.of(s, context.window().getEnd(), evalTime, cnt));
		}
	}
}

