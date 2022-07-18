package com.my.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class HBaseReader extends RichSourceFunction<Tuple2<String, String>> {
  private static final Logger logger = LoggerFactory.getLogger(HBaseReader.class);
  private static final String source_table = "test";
  private static final String source_tabble_column_familiy = "cf1";

  private Connection conn = null;
  private Table table = null;
  private Scan scan = null;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    conn = getHBaseConn();
    table = conn.getTable(TableName.valueOf(source_table));
    scan = new Scan();
    scan.addFamily(Bytes.toBytes(source_tabble_column_familiy));
  }


  @Override
  public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
    ResultScanner rs = table.getScanner(scan);
    Iterator<Result> iterator = rs.iterator();
    while (iterator.hasNext()) {
      Result result = iterator.next();
      String rowkey = Bytes.toString(result.getRow());
      StringBuffer sb = new StringBuffer();
      for (Cell cell : result.listCells()) {
        String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
        sb.append(value).append(",");
      }
      String valueString = sb.replace(sb.length() - 1, sb.length(), "").toString();
      Tuple2<String, String> tuple2 = new Tuple2<>();
      tuple2.setFields(rowkey, valueString);
      sourceContext.collect(tuple2);
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
      logger.error("Close HBase Exception:", e.toString());
    }
  }

  private static Connection getHBaseConn() throws IOException {
    return ConnectionFactory.createConnection(HBaseConfigurationUtil.getHBaseConfiguration());
  }
}
