package com.soul.apitest.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class TableTest2_CommonApi {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从文件读取
        tableEnv.connect(new FileSystem().path("src/main/resources/sensor.txt"))
                .withFormat(new Csv())
                .withSchema(
                        new Schema()
                                .field("id", DataTypes.STRING())
                                .field("timestamp", DataTypes.BIGINT())
                                .field("temperature", DataTypes.DOUBLE())
                ).createTemporaryTable("inputTable");
        Table inputTable = tableEnv.from("inputTable");
//        inputTable.printSchema();
//        tableEnv.toAppendStream(inputTable, Row.class).print();

        // Table API
        // 查询转换
        Table resultTable = inputTable.select("id, temperature").filter("id === 'sensor_6'");
        // 聚合统计
        Table aggTable = inputTable.groupBy("id").select("id, id.count as count, temperature.avg as avgTemp");

        // SQL
        Table sqlResultTable = tableEnv.sqlQuery("select id, temperature from inputTable where id = 'sensor_6'");
        Table sqlAggTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temperature) as avgTemp from inputTable group by id");

        // 打印
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toRetractStream(aggTable, Row.class).print("agg"); // (true,sensor_1,1,36.8) true表示为将要插入的新数据 false表示
        tableEnv.toRetractStream(sqlResultTable, Row.class).print("sqlResultTable");
        tableEnv.toRetractStream(sqlAggTable, Row.class).print("sqlAgg");

        // 输出到文件系统
        tableEnv.connect(new FileSystem().path("src/main/resources/output.txt"))
                .withFormat(new Csv())
                .withSchema(
                        new Schema()
                                .field("id", DataTypes.STRING())
                                .field("temperature", DataTypes.DOUBLE())
                ).createTemporaryTable("outputTable");
        resultTable.insertInto("outputTable");
        // 往文件写入只支持AppendStream，aggTable为RetractStream

        env.execute();
    }
}
