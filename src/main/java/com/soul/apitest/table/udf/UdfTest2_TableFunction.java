package com.soul.apitest.table.udf;

import com.soul.apitest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class UdfTest2_TableFunction {

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

        // 自定义表函数，将id拆分，输出(word, length)
        // table api
        Split split = new Split("_");

        // 需要在环境中注册UDF
        tableEnv.registerFunction("split", split);
        Table resultTable = sensorTable.joinLateral("split(id) as (word, length)")
                .select("id, ts, word, length");

        // SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id, ts, word, length " +
                "from sensor, lateral table(split(id)) as split_id(word, length)");

        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("resultSql");

        env.execute();
    }

    public static class Split extends TableFunction<Tuple2<String, Integer>> {

        private String separator = ",";

        public Split(String separator) {
            this.separator = separator;
        }

        public void eval(String str) {
            for (String s : str.split(this.separator)) {
                collect(new Tuple2<>(s, s.length()));
            }
        }
    }
}
