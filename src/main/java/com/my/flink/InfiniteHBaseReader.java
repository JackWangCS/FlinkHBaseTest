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
import java.util.concurrent.TimeUnit;

public class InfiniteHBaseReader extends RichSourceFunction<Tuple2<String, String>> {
  private static final Logger logger = LoggerFactory.getLogger(InfiniteHBaseReader.class);

  private volatile boolean running = true;
  private final String sourceTable;
  private final String sourceTableColumnFamiliy;

  private Connection conn = null;
  private Table table = null;
  private Scan scan = null;

  InfiniteHBaseReader(String sourceTable, String scanColumnFamily) {
    this.sourceTable = sourceTable;
    this.sourceTableColumnFamiliy = scanColumnFamily;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    initConnection();
  }

  private void initConnection() throws IOException {
    conn = getHBaseConn();
    table = conn.getTable(TableName.valueOf(sourceTable));
    scan = new Scan();
    scan.addFamily(Bytes.toBytes(sourceTableColumnFamiliy));
    logger.info("initialize HBase connection");
  }


  @Override
  public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
    while (running) {
      logger.info("start to scan {}:{}", sourceTable, sourceTableColumnFamiliy);
      if (conn == null || conn.isClosed()) {
        initConnection();
      }
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
      logger.info("finish scan {}:{}", sourceTable, sourceTableColumnFamiliy);

      TimeUnit.SECONDS.sleep(10);
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
