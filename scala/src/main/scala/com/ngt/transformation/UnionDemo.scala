package com.ngt.transformation

import org.apache.flink.streaming.api.scala._

/**
 * @author ngt
 * @create 2021-02-02 1:49
 * union 要求流的数据类型必须相同
  Connect与 Union 区别：
  1． Union之前两个流的类型必须是一样，Connect可以不一样，在之后的coMap中再去调整成为一样的。
  2. Connect只能操作两个流，Union可以操作多个。
 */
object UnionDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines1: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)
    val lines2: DataStream[String] = env.socketTextStream("192.168.31.8", 9999)

    // 两个 source 分支
    lines1.union(lines2).print()

    // 只有一个source分支，重复发送两次
    lines1.union(lines1).print()

    val elements: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 0)
    // Union 要求流的数据类型必须相同
//    lines1.union(elements)
    env.execute()
  }
}
