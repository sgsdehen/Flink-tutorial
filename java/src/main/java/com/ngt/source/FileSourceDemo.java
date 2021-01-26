package com.ngt.source;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * @author ngt
 * @create 2021-01-25 14:53
 * 将变化的文件从头读取，不会增量读取
 */
public class FileSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String path = "data";

        /* readFile
            1. readFile 多并行的Source
            2. FileProcessingMode ：
               PROCESS_ONCE模式Source只读取文件中的数据一次，读取完成后，程序退出
               PROCESS_CONTINUOUSLY模式Source会一直监听指定的文件，需要指定检测该文件是否发生变化的时间间隔
            3. PROCESS_CONTINUOUSLY模式，文件的内容发生变化后，会将以前的内容和新的内容全部都读取出来，进而造成数据重复读取
         */
        DataStreamSource<String> lines = env.readFile(new TextInputFormat(null), path,
                FileProcessingMode.PROCESS_CONTINUOUSLY, 2000);

        int parallelism = lines.getParallelism();
        System.out.println("readFilePallelism:" + parallelism);
        // readFilePallelism:8
//        lines.print();
        /*
        6> dddd
        7> eeee
        3> 5555
        8> 1111
        1> 3333
        4> aaaa
        2> 4444
        4> bbbb
        8> 2222
        5> cccc
        8> 1111
        2> 4444
        1> 3333
        8> 2222
        2> 5555
        3> 6666
         */


        /* readTextFile
           1. 可以从指定的目录或文件读取数据，默认使用的是TextInputFormat格式读取数据
           2. 有限的数据源，数据读完后，程序就会退出，不能一直运行
           3. 该方法底层调用的是readFile方法，FileProcessingMode为PROCESS_ONCE
           4. 因此也是多并行的方法
         */
        DataStreamSource<String> readTextFile = env.readTextFile("data");
        int parallelism1 = readTextFile.getParallelism();
        System.out.println("readTextFilePallelism:" + parallelism1);

        readTextFile.print();
        env.execute();
    }
}
