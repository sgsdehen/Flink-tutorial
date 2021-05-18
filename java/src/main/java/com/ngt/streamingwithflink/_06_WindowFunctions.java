package com.ngt.streamingwithflink;

import com.ngt.streamingwithflink.util.SensorReading;
import com.ngt.streamingwithflink.util.SensorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.time.Duration;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author ngt
 * @create 2021-05-18 9:58
 */
public class _06_WindowFunctions {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 1. ReduceFunction 使用lambda表达式
		env.addSource(new SensorSource())
				.assignTimestampsAndWatermarks(WatermarkStrategy
						.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(1))
						.withTimestampAssigner((element, timestamp) -> element.timestamp))
				.map(data -> Tuple2.of(data.id, data.temperature))
				.keyBy(data -> data.f0)
				.window(TumblingEventTimeWindows.of(Time.seconds(3)))
				.reduce((r1, r2) -> Tuple2.of(r1.f0, Math.min(r1.f1, r2.f1)))
				.print();

		// 1. ReduceFunction 使用类
		env.addSource(new SensorSource())
				.assignTimestampsAndWatermarks(WatermarkStrategy
						.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(1))
						.withTimestampAssigner((element, timestamp) -> element.timestamp))
				.map(data -> Tuple2.of(data.id, data.temperature))
				.keyBy(data -> data.f0)
				.window(TumblingEventTimeWindows.of(Time.seconds(3)))
				.reduce(new MinTempFunction())
				.print();

		// 2. aggregate 求平均值
		env.addSource(new SensorSource())
				.assignTimestampsAndWatermarks(WatermarkStrategy
						.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(1))
						.withTimestampAssigner((element, timestamp) -> element.timestamp))
				.map(data -> Tuple2.of(data.id, data.temperature))
				.keyBy(data -> data.f0)
				.window(TumblingEventTimeWindows.of(Time.seconds(3)))
				.aggregate(new AvgTempFunction())
				.print();


		// 3. 全量聚合 HighAndLowTempProcessFunction 求最值
		env.addSource(new SensorSource())
				.assignTimestampsAndWatermarks(WatermarkStrategy
						.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(1))
						.withTimestampAssigner((element, timestamp) -> element.timestamp))
				.keyBy(data -> data.id)
				.window(TumblingEventTimeWindows.of(Time.seconds(5)))
				.process(new HighAndLowTempProcessFunction())
				.print();

		// 4. 增量聚合 HighAndLowTempProcessFunction 求最值
		env.addSource(new SensorSource())
				.assignTimestampsAndWatermarks(WatermarkStrategy
						.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(1))
						.withTimestampAssigner((element, timestamp) -> element.timestamp))
				// 因为 ReduceFunction 的输入和输出的类型必须保持一致，因此要进行类型转换
				.map(r -> Tuple3.of(r.id, r.temperature, r.temperature))
				.keyBy(data -> data.f0)
				.window(TumblingEventTimeWindows.of(Time.seconds(5)))
				.reduce((r1, r2) -> Tuple3.of(r1.f0, Math.min(r1.f1, r2.f1), Math.max(r1.f1, r2.f1)), new AssignWindowEndProcessFunction())
				.print();

	}

	// 求最小值
	static class MinTempFunction implements ReduceFunction<Tuple2<String, Double>> {
		@Override
		public Tuple2<String, Double> reduce(Tuple2<String, Double> value1, Tuple2<String, Double> value2) throws Exception {
			return Tuple2.of(value1.f0, Math.min(value1.f1, value2.f1));
		}
	}

	// 求平均值
	static class AvgTempFunction implements AggregateFunction<Tuple2<String, Double>, Tuple3<String, Double, Integer>, Tuple2<String, Double>> {
		@Override
		public Tuple3<String, Double, Integer> createAccumulator() {
			return Tuple3.of("", 0.0, 0);
		}

		@Override
		public Tuple3<String, Double, Integer> add(Tuple2<String, Double> value, Tuple3<String, Double, Integer> accumulator) {
			return Tuple3.of(value.f0, value.f1 + accumulator.f1, accumulator.f2 + 1);
		}

		@Override
		public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> accumulator) {
			return Tuple2.of(accumulator.f0, accumulator.f1 / accumulator.f2);
		}

		@Override
		public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> a, Tuple3<String, Double, Integer> b) {
			return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2);
		}
	}

	static class MinMaxTemp {
		private String id;
		private Double min;
		private Double max;
		private Long ends;

		public MinMaxTemp() {
		}

		public MinMaxTemp(String id, Double min, Double max, Long ends) {
			this.id = id;
			this.min = min;
			this.max = max;
			this.ends = ends;
		}

		@Override
		public String toString() {
			return "MinMaxTemp{" +
					"id='" + id + '\'' +
					", min=" + min +
					", max=" + max +
					", ends=" + ends +
					'}';
		}
	}

	static class HighAndLowTempProcessFunction extends ProcessWindowFunction<SensorReading, MinMaxTemp, String, TimeWindow> {
		@Override
		public void process(String s,
							Context context,
							Iterable<SensorReading> elements,
							Collector<MinMaxTemp> out) throws Exception {
			Stream<Double> temps = StreamSupport.stream(elements.spliterator(), false).map(data -> data.temperature);

			long windowEnd = context.window().getEnd();
			out.collect(new MinMaxTemp(s, temps.min(Double::compareTo).get(), temps.max(Double::compareTo).get(), windowEnd));
		}
	}

	static class AssignWindowEndProcessFunction extends ProcessWindowFunction<Tuple3<String, Double, Double>, MinMaxTemp, String, TimeWindow> {
		@Override
		public void process(String s,
							Context context,
							Iterable<Tuple3<String, Double, Double>> elements,
							Collector<MinMaxTemp> out) throws Exception {
			Tuple3<String, Double, Double> minMax = elements.iterator().next();
			long windowEnd = context.window().getEnd();
			out.collect(new MinMaxTemp(s, minMax.f1, minMax.f2, windowEnd));
		}
	}
}
