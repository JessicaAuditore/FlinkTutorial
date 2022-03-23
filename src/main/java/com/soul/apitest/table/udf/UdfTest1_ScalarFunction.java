package com.soul.apitest.table.udf;

import com.soul.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

public class UdfTest1_ScalarFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        Table sensorTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp");

        // table api
        HashCode hashCode = new HashCode(23);

        // 需要在环境中注册UDF
        tableEnv.registerFunction("hashCode", hashCode);
        Table resultTable = sensorTable.select("id , ts, hashCode(id)");

        // SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id, ts, hashCode(id) from sensor");

        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("resultSql");

        env.execute();
    }

    public static class HashCode extends ScalarFunction {

        private int factor = 13;

        public HashCode(int factor) {
            this.factor = factor;
        }

        public int eval(String str) {
            return str.hashCode() * factor;
        }
    }
}
