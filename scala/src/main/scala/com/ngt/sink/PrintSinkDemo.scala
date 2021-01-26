package com.ngt.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @author ngt
 * @create 2021-01-26 21:33
 */
object PrintSinkDemo {
  def main(args: Array[String]): Unit = {
    val configuration: Configuration = new Configuration()
    configuration.setInteger("rest.port", 8181)
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)
    val words: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    words.addSink(new MyPrintSink)
    env.execute()
  }

  case class MyPrintSink() extends RichSinkFunction[String] {
    var indexOfThisSubtask: Int = _

    override def open(parameters: Configuration): Unit = {
      indexOfThisSubtask = getRuntimeContext.getIndexOfThisSubtask
      super.open(parameters)
    }

    override def invoke(value: String, context: SinkFunction.Context): Unit = {
      println(indexOfThisSubtask + 1 + "> " + value)
    }
  }

}
