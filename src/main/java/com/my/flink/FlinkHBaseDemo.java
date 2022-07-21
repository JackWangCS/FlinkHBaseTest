package com.my.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkHBaseDemo {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    ParameterTool parameterTool = ParameterTool.fromArgs(args);

    String zkQuorum = parameterTool.get("zk");
    String tableName = parameterTool.get("table", "test");


    tableEnv.executeSql("CREATE TABLE test(rowkey STRING, cf1 ROW<name STRING, gender STRING>, PRIMARY KEY (rowkey) NOT ENFORCED) " +
        " WITH (\n" +
        "'connector' = 'hbase-1.4', \n" +
        "'table-name' = '" + tableName + "', \n" +
        "'zookeeper.quorum' = '" + zkQuorum + "')");


    final Table result = tableEnv.sqlQuery("SELECT * FROM test");

    tableEnv.toDataStream(result).print();

    env.execute();

  }

}
