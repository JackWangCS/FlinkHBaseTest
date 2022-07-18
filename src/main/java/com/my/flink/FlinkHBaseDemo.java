package com.my.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkHBaseDemo {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


    tableEnv.executeSql("CREATE TABLE test(rowkey STRING, cf1 ROW<name STRING, gender STRING>, PRIMARY KEY (rowkey) NOT ENFORCED) " +
        " WITH (\n" +
        "'connector' = 'hbase-1.4', \n" +
        "'table-name' = 'test', \n" +
        "'zookeeper.quorum' = 'ip-172-31-25-182.ec2.internal')");


    final Table result = tableEnv.sqlQuery("SELECT * FROM test");

    tableEnv.toDataStream(result).print();

    env.execute();

  }

}
