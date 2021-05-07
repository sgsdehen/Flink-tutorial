package com.ngt.state

import org.apache.commons.codec.Charsets
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}


import java.io.RandomAccessFile
import java.util
import java.util.concurrent.TimeUnit

/**
 * @author ngt
 * @create 2021-02-09 8:46
 */
object MyAtleastOnceSourceDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)))
    env.enableCheckpointing(5000)

    val errorDate: DataStream[String] = lines.map(new MapFunction[String, String] {
      override def map(value: String): String = {
        if (value.startsWith("error")) {
          throw new RuntimeException()
        }
        value
      }
    })
    val lines2: DataStream[String] = env.addSource(new MyAtleastOnceSource("data/myck"))
    errorDate.union(lines2).print()
    env.execute()

  }

  case class MyAtleastOnceSource(path: String) extends RichParallelSourceFunction[String] with CheckpointedFunction {


    private var offset: Long = _ // 需要序列化操作
    private var listState: ListState[Long] = _ // 不需要序列化操作
    //private var path: String = _ // 需要序列化操作
    private var flag: Boolean = _ // 需要序列化操作


    // 在checkpoint 是会执行一次，会周期性的执行
    override def snapshotState(context: FunctionSnapshotContext): Unit = {
      listState.clear() // 清除原来的偏移量
      listState.add(offset) // 写入新的偏移量
    }

    // 初始化状态或恢复状态执行一次，在run方法执行之前执行
    override def initializeState(context: FunctionInitializationContext): Unit = {
      val listStateDescriptor: ListStateDescriptor[Long] =
        new ListStateDescriptor[Long]("listSate", classOf[Long])
      listState = context.getOperatorStateStore().getListState(listStateDescriptor)
      if (context.isRestored()) { //当前的状态是否恢复完成
        // 从list中恢复偏移量
        val iterable: util.Iterator[Long] = listState.get().iterator()
        while (iterable.hasNext) {
          offset = iterable.next()
        }
      }
    }

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      val indexOfThisSubtask: Int = getRuntimeContext().getIndexOfThisSubtask()
      val randomAccessFile: RandomAccessFile = new RandomAccessFile(path + "/" + indexOfThisSubtask + ".txt", "r")

      randomAccessFile.seek(offset)
      while (flag) {
        var line: String = randomAccessFile.readLine()
        if (line != null) {
          //
          line = new String(line.getBytes(Charsets.ISO_8859_1), Charsets.UTF_8)
          synchronized {
            offset = randomAccessFile.getFilePointer()
            ctx.collect(indexOfThisSubtask + ".txt: " + line)
          }
        } else {
          TimeUnit.SECONDS.sleep(5)
        }
      }
    }

    override def cancel(): Unit = {
      flag = false
    }
  }

}
