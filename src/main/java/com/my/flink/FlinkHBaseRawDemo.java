package com.my.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkHBaseRawDemo {

  public static void main(String[] args) throws Exception {


    ParameterTool parameterTool = ParameterTool.fromArgs(args);

    String tableName = parameterTool.get("table", "test");
    String columnFamily = parameterTool.get("cf", "cf1");



    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.addSource(new InfiniteHBaseReader(tableName, columnFamily))
        .print();

    env.execute();

  }

}
