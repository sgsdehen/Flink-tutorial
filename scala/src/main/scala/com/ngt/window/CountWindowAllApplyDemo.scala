package com.ngt.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author ngt
 * @create 2021-02-03 11:41
 *         apply 不使用增量操作，将窗口内的数据先存起来，放在WindowState中
 *         对输入的数据每 5 个一组按照非降的顺序输出
 */
object CountWindowAllApplyDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val nums: DataStream[Int] = lines.map(_.toInt)
    // 不分组，将整体当成一个组。并行度为1
    val window: AllWindowedStream[Int, GlobalWindow] = nums.countWindowAll(5)

    window.apply(new AllWindowFunction[Int, Int, GlobalWindow] {
      override def apply(window: GlobalWindow, input: Iterable[Int], out: Collector[Int]): Unit = {
        // 每 5 个数据进行非降排序
        //val list: Array[Int] = new Array[Int](5)
        val list: ListBuffer[Int] = ListBuffer()
        for (elem <- input) {
          list.append(elem)
        }
        val sortList: ListBuffer[Int] = list.sortBy(data => data)(Ordering.Int)
        for (elem <- sortList) {
          out.collect(elem)
        }
      }
    }).print()
      .setParallelism(1)  // 此处必须将print的并行度设置为1 才能按照顺序输出
    env.execute()
  }
}
