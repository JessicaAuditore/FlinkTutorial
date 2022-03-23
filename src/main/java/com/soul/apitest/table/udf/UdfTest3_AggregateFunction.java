package com.soul.apitest.table.udf;

import com.soul.apitest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.In;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class UdfTest3_AggregateFunction {

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

        // 自定义聚合函数，求当前传感器的平均温度值
        // table api
        AvgTemp avgTemp = new AvgTemp();

        // 需要在环境中注册UDF
        tableEnv.registerFunction("avgTemp", avgTemp);
        Table resultTable = sensorTable.groupBy("id").aggregate("avgTemp(temp) as avgT")
                .select("id, avgT");

        // SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id, avgTemp(temp) " +
                "from sensor group by id");

        tableEnv.toRetractStream(resultTable, Row.class).print("result");
        tableEnv.toRetractStream(resultSqlTable, Row.class).print("resultSql");

        env.execute();
    }

    public static class AvgTemp extends AggregateFunction<Double, Tuple2<Double, Integer>> {

        @Override
        public Double getValue(Tuple2<Double, Integer> doubleIntegerTuple2) {
            return doubleIntegerTuple2.f0 / doubleIntegerTuple2.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        // 实现一个accumulate方法 (状态, 输入)
        public void accumulate(Tuple2<Double, Integer> accumulator, Double temp) {
            accumulator.f0 += temp;
            accumulator.f1++;
        }
    }
}
