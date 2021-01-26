package com.ngt.source

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * @author ngt
 * @create 2021-01-26 19:03
 */
object FileSourceDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val path: String = "data"

    /* readFile
        1. readFile 多并行的Source
        2. FileProcessingMode ：
           PROCESS_ONCE模式Source只读取文件中的数据一次，读取完成后，程序退出
           PROCESS_CONTINUOUSLY模式Source会一直监听指定的文件，需要指定检测该文件是否发生变化的时间间隔
        3. PROCESS_CONTINUOUSLY模式，文件的内容发生变化后，会将以前的内容和新的内容全部都读取出来，进而造成数据重复读取
     */

    val lines: DataStream[String] = env.readFile(new TextInputFormat(null), path,
      FileProcessingMode.PROCESS_CONTINUOUSLY, 2000)
    lines.print()
    val parallelism: Int = lines.parallelism
    println("readFilePallelism:" + parallelism)


    /* readTextFile
       1. 可以从指定的目录或文件读取数据，默认使用的是TextInputFormat格式读取数据
       2. 有限的数据源，数据读完后，程序就会退出，不能一直运行
       3. 该方法底层调用的是readFile方法，FileProcessingMode为PROCESS_ONCE
       4. 因此也是多并行的方法
    */
    val readTextFile: DataStream[String] = env.readTextFile("data")
    val parallelism1: Int = readTextFile.parallelism
    println("readTextFilePallelism:" + parallelism1)

    readTextFile.print()
    env.execute()
  }
}
