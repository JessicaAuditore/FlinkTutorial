package com.soul.apitest.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class TableTest3_KafkaPipeLine {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从kafka读取
        tableEnv.connect(new Kafka()
                        .version("universal").topic("sensor")
                        .property("zookeeper.connect", "192.168.50.16:2181")
                        .property("bootstrap.servers", "192.168.50.16:9091,192.168.50.16:9092,192.168.50.16:9093")
                ).withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
                ).createTemporaryTable("inputTable");
        // 简单转换
        Table inputTable = tableEnv.from("inputTable");
        Table resultTable = inputTable.select("id, temperature").filter("id === 'sensor_6'");

        // 输出到kafka不同topic
        tableEnv.connect(new Kafka()
                        .version("universal").topic("sinkTest1")
                        .property("zookeeper.connect", "192.168.50.16:2181")
                        .property("bootstrap.servers", "192.168.50.16:9091,192.168.50.16:9092,192.168.50.16:9093")
                ).withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temperature", DataTypes.DOUBLE())
                ).createTemporaryTable("outputTable");
        resultTable.insertInto("outputTable");

        env.execute();
    }
}
