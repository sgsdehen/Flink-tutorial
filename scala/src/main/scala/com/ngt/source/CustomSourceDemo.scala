package com.ngt.source

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @author ngt
 * @create 2021-01-26 18:24
 */
object CustomSourceDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    println("StreamExecutionEnvironmentParallelism: " + env.getParallelism)

    val source1: DataStream[String] = env.addSource(new MySource1)
    println("SourceFunction的并行度：" + source1.parallelism)
    source1.print()


    val source2: DataStream[String] = env.addSource(new MySource2)
    println("SourceFunction的并行度：" + source2.parallelism)
    source2.print()

    env.execute()

  }

  // 实现 SourceFunction 接口的Source是非并行，有限的数据量
  case class MySource1() extends SourceFunction[String]{
    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      val words: List[String] = List("a", "b", "c", "d", "e")
      for (elem <- words) {
        ctx.collect(elem)
      }
    }

    override def cancel(): Unit = ???
  }

  // 实现 SourceFunction 接口Source是多并行的，有限的数据量
  case class MySource2() extends ParallelSourceFunction[String]{
    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      val words: List[String] = List("a", "b", "c", "d", "e")
      for (elem <- words) {
        ctx.collect(elem)
      }
    }

    override def cancel(): Unit = ???
  }

}

/*
  StreamExecutionEnvironmentParallelism: 4
  SourceFunction的并行度：1
  SourceFunction的并行度：4
 */