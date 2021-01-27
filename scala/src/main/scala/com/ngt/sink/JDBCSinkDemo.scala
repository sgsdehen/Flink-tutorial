package com.ngt.sink

import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink}
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * @author ngt
 * @create 2021-01-26 22:45
 */
object JDBCSinkDemo {

  // JdbcSink 暂时不支持 Scala 操作？ 没有测试通过，还需要查询相关资料
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val words: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)
    env.enableCheckpointing(5000)

    val summed: DataStream[(String, Int)] = words
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)

    summed.print()
    // 此处需要类型转换操作
    val outputStream = summed.asInstanceOf[SingleOutputStreamOperator[Tuple2[String, Integer]]]

    outputStream.addSink(JdbcSink.sink("insert into wordcount(word,count) values (?,?) on duplicate key update count = ?",
      (ps, t) => {
        // word 必须是主键才能达到更新数据的目的
        ps.setString(1, t.f0)
        ps.setInt(2, t.f1)
        ps.setInt(3, t.f1)
      },
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:mysql://hadoop102:3306/test")
        .withDriverName("com.mysql.jdbc.Driver")
        .withUsername("root")
        .withPassword("123456")
        .build()))
    env.execute()
  }

  // 使用自定义Sink的方式实现JDBC操作
  def main2(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val words: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    val summed: DataStream[(String, Int)] = words
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)

    summed.addSink(new MyJdbcSinkFunc())
    env.execute()
  }

  class MyJdbcSinkFunc extends RichSinkFunction[(String, Int)] {
    var conn: Connection = _
    var updateStmt: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456")
      updateStmt = conn.prepareStatement("insert into wordcount(word,count) values (?,?) on duplicate key update count = ?")
    }

    override def invoke(value: (String, Int), context: SinkFunction.Context): Unit = {
      // 查到就更新，没查到就写入
      updateStmt.setString(1, value._1)
      updateStmt.setInt(2, value._2)
      updateStmt.setInt(3, value._2)
      updateStmt.execute()
    }

    override def close(): Unit = {
      updateStmt.close()
      conn.close()
    }
  }

}
