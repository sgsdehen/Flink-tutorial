package com.ngt.demo.hotitems;

import com.ngt.demo.hotitems.beans.ItemViewCount;
import com.ngt.demo.hotitems.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * @author ngt on 2021-05-24 9:45
 * @version 1.0
 */
public class HotItems {
	public static void main(String[] args) throws Exception {
		// 1. 创建执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.getConfig().setAutoWatermarkInterval(1000L);

		// 2. 读取数据，创建DataStream
		DataStreamSource<String> inputStream = env.readTextFile("data/UserBehavior.csv");

		// 3. 转换为POJO，分配时间戳和watermark
		SingleOutputStreamOperator<UserBehavior> dataStream = inputStream.map(line -> {
			String[] fields = line.split(",");
			return new UserBehavior(Long.parseLong(fields[0]), Long.parseLong(fields[1]), Integer.parseInt(fields[2]), fields[3], Long.parseLong(fields[4]));
		}).assignTimestampsAndWatermarks(WatermarkStrategy
				.<UserBehavior>forBoundedOutOfOrderness(Duration.ZERO)
				.withTimestampAssigner((data, timestamp) -> data.getTimestamp() * 1000L));

		// 4. 分组开窗聚合，得到每个窗口内各个商品的count值
		DataStream<ItemViewCount> windowAggStream = dataStream
				.filter(data -> "pv".equals(data.getBehavior()))    // 过滤pv行为
				.keyBy(UserBehavior::getItemId)    // 按商品ID分组
				.window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))    // 开滑窗
				.aggregate(new ItemCountAgg(), new WindowItemCountResult());

		// 5. 收集同一窗口的所有商品count数据，排序输出top n
		DataStream<String> resultStream = windowAggStream
				.keyBy(ItemViewCount::getWindowEnd)      // 按照窗口分组
				.process(new TopNHotItems(5));   // 用自定义处理函数排序取前5

		resultStream.print();

		env.execute("hot items analysis");

	}

	static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {
		@Override
		public Long createAccumulator() {
			return 0L;
		}

		@Override
		public Long add(UserBehavior value, Long accumulator) {
			return accumulator + 1;
		}

		@Override
		public Long getResult(Long accumulator) {
			return accumulator;
		}

		@Override
		public Long merge(Long a, Long b) {
			return a + b;
		}
	}

	static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {
		@Override
		public void apply(Long key,
						  TimeWindow window,
						  Iterable<Long> input,
						  Collector<ItemViewCount> out) throws Exception {
			Long count = input.iterator().next();
			out.collect(new ItemViewCount(key, window.getEnd(), count));
		}
	}

	static class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {
		// 定义属性，top n的大小
		private  Integer topSize;

		public TopNHotItems(Integer topSize) {
			this.topSize = topSize;
		}

		// 定义列表状态，保存当前窗口内所有输出的ItemViewCount
		private transient ListState<ItemViewCount> itemViewCountListState;

		@Override
		public void open(Configuration parameters) throws Exception {
			itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list", ItemViewCount.class));
		}

		@Override
		public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
			// 每来一条数据，存入List中，并注册定时器
			itemViewCountListState.add(value);
			ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
			// 定时器触发，当前已收集到所有数据，排序输出
			ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());

			itemViewCounts.sort((o1, o2) -> o2.getCount().intValue() - o1.getCount().intValue());

			// 将排名信息格式化成String，方便打印输出
			StringBuilder resultBuilder = new StringBuilder();
			resultBuilder.append("===================================\n");
			resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");

			// 遍历列表，取top n输出
			for (int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++) {
				ItemViewCount currentItemViewCount = itemViewCounts.get(i);
				resultBuilder.append("NO ").append(i + 1).append(":")
						.append(" 商品ID = ").append(currentItemViewCount.getItemId())
						.append(" 热门度 = ").append(currentItemViewCount.getCount())
						.append("\n");
			}
			resultBuilder.append("===============================\n\n");

			// 控制输出频率
			TimeUnit.MILLISECONDS.sleep(1000L);
			out.collect(resultBuilder.toString());
		}
	}

}
