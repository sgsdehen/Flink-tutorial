package com.ngt.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.com.google.common.base.Charsets;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;
import java.util.concurrent.TimeUnit;

/**
 * @author ngt
 * @create 2021-01-31 4:14
 */
public class MyAtleastOnceSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));

        DataStreamSource<String> lines1 = env.socketTextStream("192.168.31.8", 8888);

        SingleOutputStreamOperator<String> errorDate = lines1.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if (value.startsWith("error")) {
                    throw new RuntimeException();
                }
                return value;
            }
        });

        //
        DataStreamSource<String> lines2 = env.addSource(new MyAtleastOnceSource("data/myck"));

        errorDate.union(lines2).print();
        env.execute();
    }

    // 自定义source

    public static class MyAtleastOnceSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {

        private Long offset;   // 需要序列化操作
        private transient ListState<Long> listState;// 不需要序列化操作
        private String path;   // 需要序列化操作
        private boolean flag;  // 需要序列化操作

        public MyAtleastOnceSource(String path) {
            this.path = path;
        }

        // 初始化状态或恢复状态执行一次，在run方法执行之前执行
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 用于保存偏移量
            ListStateDescriptor<Long> listStateDescriptor = new ListStateDescriptor<>("listSate", Long.class);
            listState = context.getOperatorStateStore().getListState(listStateDescriptor);

            if (context.isRestored()) {//当前的状态是否恢复完成
                // 从list中恢复偏移量
                Iterable<Long> iterable = listState.get();
                for (Long l : iterable) {
                    offset = l;
                }
            }
        }

        // 在checkpoint 是会执行一次，会周期性的执行
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            listState.clear();     // 清除原来的偏移量
            listState.add(offset); // 写入新的偏移量
        }


        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            RandomAccessFile randomAccessFile = new RandomAccessFile(path + "/" + indexOfThisSubtask + ".txt", "r");
            randomAccessFile.seek(offset); // 从指定的位置开始读取数据
            while (flag) {
                String line = randomAccessFile.readLine();
                if (line != null) {
                    // 防止中文乱码的出现
                    line = new String(line.getBytes(Charsets.ISO_8859_1), Charsets.UTF_8);
                    synchronized (ctx.getCheckpointLock()) {
                        // 读取文件之后更新偏移量
                        offset = randomAccessFile.getFilePointer();
                        ctx.collect(indexOfThisSubtask + ".txt: " + line);
                    }
                } else {
                    TimeUnit.SECONDS.sleep(5);
                }
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
