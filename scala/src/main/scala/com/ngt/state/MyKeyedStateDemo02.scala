package com.ngt.state

import org.apache.flink.api.common.functions.{FlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.io.{File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.concurrent.TimeUnit
import scala.collection.mutable

/**
 * @author ngt
 * @create 2021-02-09 6:26
 */
object MyKeyedStateDemo02 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("192.168.31.8", 8888)

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)))
    env.enableCheckpointing(5000)

    val wordAndOne: DataStream[(String, Int)] = lines.flatMap(new FlatMapFunction[String, (String, Int)] {
      override def flatMap(value: String, out: Collector[(String, Int)]): Unit = {
        val words: Array[String] = value.split(" ")
        for (elem <- words) {
          if ("error".equals(elem)) {
            throw new RuntimeException("Exception")
          }
          out.collect((elem, 1))
        }
      }
    })

    // 不调用 sum，因为其会存在状态,自己实现状态存储， 使用 RichMapFunction 便于获取运行时上下文
    wordAndOne.keyBy(_._1)
      .map(new RichMapFunction[(String, Int), (String, Int)] {
        var counter: mutable.HashMap[String, Int] = _

        // 使用open方法，初始化 hashmap 或者恢复历史状态
        override def open(parameters: Configuration): Unit = {
          // 为了将不同的subTask写入对应的文件中
          val indexOfThisSubtask: Int = getRuntimeContext.getIndexOfThisSubtask
          val ckFile: File = new File("data/myck/" + indexOfThisSubtask)
          if (ckFile.exists()) {
            val fileInputStream: FileInputStream = new FileInputStream(ckFile)
            val objectInputStream: ObjectInputStream = new ObjectInputStream(fileInputStream)
            val counter: mutable.HashMap[String, Int] = objectInputStream.readObject().asInstanceOf[mutable.HashMap[String, Int]]
          }
          else {
            counter = new mutable.HashMap[String, Int]()
          }
          // 定时保存策略，使用新的线程定期将内存的数据写到磁盘
          new Thread(() => {
            while (true) {
              try {
                TimeUnit.SECONDS.sleep(5)
                if (!ckFile.exists()) {
                  ckFile.createNewFile()
                }
                val objectOutputStream: ObjectOutputStream = new ObjectOutputStream(new FileOutputStream(ckFile))
                objectOutputStream.writeObject(counter)
                objectOutputStream.flush()
                objectOutputStream.close()
              } catch {
                case ex: Exception => ex.printStackTrace()
              }
            }
          }).start()
        }

        override def map(value: (String, Int)): (String, Int) = {
          val word: String = value._1
          val count: Int = value._2
          var historyCount: Int = 0
          val maybeInt: Option[Int] = counter.get(word)
          if (maybeInt.isDefined) {
            historyCount = maybeInt.get
          }
          val sum: Int = historyCount + count
          counter.put(word, sum)
          (word, sum)
        }
      })
      .print()

    env.execute()
  }
}
