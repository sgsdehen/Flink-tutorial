package com.ngt.util;

import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.Cell;
//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author ngt
 * @create 2021-02-02 20:26
 */
public class HBaseUtil {


    public class HBaseWriter extends RichSinkFunction<String> {

        private org.apache.hadoop.conf.Configuration config;
        private String tablename;
        private String familyname;


        private Connection conn;
        private BufferedMutator mutator;
        private Integer count = 0;

        public HBaseWriter() {
        }

        public HBaseWriter(org.apache.hadoop.conf.Configuration config, String tablename, String familyname) {
            this.config = config;
            this.tablename = tablename;
            this.familyname = familyname;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = ConnectionFactory.createConnection(config);
            TableName tableName = TableName.valueOf(tablename);
            BufferedMutatorParams params = new BufferedMutatorParams(tableName);

            //设置缓存1m，当达到1m时数据会自动刷到hbase
            params.writeBufferSize(1024 * 1024); //设置缓存的大小
            mutator = conn.getBufferedMutator(params);
            count = 0;
        }


        @Override
        public void invoke(String value, SinkFunction.Context context) throws Exception {
            String cf1 = this.familyname;
            String[] array = value.split(",");
            Put put = new Put(Bytes.toBytes(array[0]));
            put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("name"), Bytes.toBytes(array[1]));
            put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("count"), Bytes.toBytes(array[2]));
            mutator.mutate(put);

            //  每满2000条刷新一下数据
            if (count >= 2000) {
                mutator.flush();
                count = 0;
            }
            count++;
        }

        @Override
        public void close() throws Exception {
            if (conn != null) conn.close();
        }
    }

    public class HBaseReader extends RichSourceFunction<Tuple2<String, String>> {

        private org.apache.hadoop.conf.Configuration config;
        private String tablename;
        private String familyname;
        private String withStartRow;
        private String withStopRow;


        private Connection conn;
        private Table table;
        private Scan scan;

        public HBaseReader(org.apache.hadoop.conf.Configuration config, String tablename, String familyname, String withStartRow, String withStopRow) {
            this.config = config;
            this.tablename = tablename;
            this.familyname = familyname;
            this.withStartRow = withStartRow;
            this.withStopRow = withStopRow;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            TableName tableName = TableName.valueOf(tablename);
            String cf1 = this.familyname;

            conn = ConnectionFactory.createConnection(config);
            table = conn.getTable(tableName);
            scan = new Scan();

            // 设置读取的范围 左闭右开
            scan.withStartRow(Bytes.toBytes(withStartRow));
            scan.withStopRow(Bytes.toBytes(withStopRow));
            scan.addFamily(Bytes.toBytes(cf1));
        }

        @Override
        public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
            ResultScanner rs = table.getScanner(scan);
            Iterator<Result> iterator = rs.iterator();
            while (iterator.hasNext()) {
                Result result = iterator.next();
                String rowKey = Bytes.toString(result.getRow());
                StringBuffer sb = new StringBuffer();
                for (Cell cell : result.listCells()) {
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    sb.append(value).append("_");
                }
                String valueString = sb.replace(sb.length() - 1, sb.length(), "").toString();
                ctx.collect(Tuple2.of(rowKey, valueString));
            }
        }

        @Override
        public void cancel() {
            try {
                if (table != null) {
                    table.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
