package com.soul.apitest.sink;


import com.soul.apitest.beans.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkTest4_Mysql {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 保证顺序输出
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.addSink(new RichSinkFunction<SensorReading>() {
            private Connection connection;
            private PreparedStatement insertStmt;
            private PreparedStatement updateStmt;

            @Override
            public void invoke(SensorReading value, Context context) throws Exception {
                // 直接执行更新语句，如果没有更新，那么就插入
                this.updateStmt.setDouble(1, value.getTemperature());
                this.updateStmt.setString(2, value.getId());
                this.updateStmt.execute();
                if (updateStmt.getUpdateCount() == 0) {
                    this.insertStmt.setString(1, value.getId());
                    this.insertStmt.setDouble(2, value.getTemperature());
                    this.insertStmt.execute();
                }
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                this.connection = DriverManager.getConnection("jdbc:mysql://101.43.147.253:3306/test", "root", "xiaokaixian");
                // 创建预编译器，有占位符，可传入参数
                this.insertStmt = this.connection.prepareStatement("INSERT INTO sensor_temp (id, temp) VALUES (?, ?)");
                this.updateStmt = this.connection.prepareStatement("UPDATE sensor_temp SET temp = ? WHERE id = ?");
            }

            @Override
            public void close() throws Exception {
                this.insertStmt.close();
                this.updateStmt.close();
                this.connection.close();
            }
        });

        env.execute();
    }
}
