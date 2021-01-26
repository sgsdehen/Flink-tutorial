package com.ngt.source

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import java.util.UUID
import java.util.concurrent.TimeUnit


/**
 * @author ngt
 * @create 2021-01-26 18:31
 */
object CustomSourceDemo1 {
  def main(args: Array[String]): Unit = {
    val configuration: Configuration = new Configuration()
    configuration.setInteger("rest.port", 8181)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)
    env.setParallelism(4)

    val source: DataStream[String] = env.addSource(new MySource())
    source.print()
    env.execute()
  }

  // 可以获取运行时上下文
  case class MySource() extends RichParallelSourceFunction[String] {

    var indexOfThisSubtask: Int = _
    var flag: Boolean = true

    override def open(parameters: Configuration): Unit = {
      indexOfThisSubtask = getRuntimeContext.getIndexOfThisSubtask
      println("subTask" + indexOfThisSubtask + " open method invoked")
      super.open(parameters)
    }

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask()
      println("subTask" + indexOfThisSubtask + " run method invoked")
      while (flag) {
        TimeUnit.SECONDS.sleep(1)
        ctx.collect("subTask" + indexOfThisSubtask + " " +
          UUID.randomUUID().toString())
      }
    }

    override def cancel(): Unit = {
      indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask()
      println("subTask" + indexOfThisSubtask + " cancel method invoked")
      flag = false
    }

    override def close(): Unit = {
      indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask()
      println("subTask " + indexOfThisSubtask + " close method invoked");
      super.close()
    }
  }
}

/*
  subTask1 open method invoked
  subTask0 open method invoked
  subTask3 open method invoked
  subTask2 open method invoked
  subTask1 run method invoked
  subTask0 run method invoked
  subTask3 run method invoked
  subTask2 run method invoked
  1> subTask0 37d8335f-c007-44ec-ba05-147a8657f9e7
  3> subTask2 4730ec43-f0d4-4cb9-a49d-2d0578774d44
  2> subTask1 cc568cf8-b1d4-4ee1-9f81-2128101cf7a2
  4> subTask3 10acf059-aba9-4bc4-bd75-247ff2ae5972
  subTask0 cancel method invoked
  subTask1 cancel method invoked
  subTask2 cancel method invoked
  subTask3 cancel method invoked
  subTask0 close method invoked
  subTask1 close method invoked
  subTask2 close method invoked
  subTask3 close method invoked
 */