package com.soul.apitest.table;

import com.soul.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class TableTest4_TimeAndWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading sensorReading) {
                return sensorReading.getTimestamp() * 1000L;
            }
        });

//        Table processTimeTable = tableEnv.fromDataStream(dataStream, "id, temperature as ts, timestamp as temp, pt.proctime");
        Table eventTimetable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp, rt.rowtime");
        tableEnv.createTemporaryView("sensor", eventTimetable);

        // group window
        // table API
        Table resultTable = eventTimetable.window(Tumble.over("10.seconds").on("rt").as("tw"))
                .groupBy("id, tw").select("id, id.count, temp.avg, tw.end");
        // SQL
        Table resultSqlTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp, tumble_end(rt, interval '10' second) " +
                "from sensor group by id, tumble(rt, interval '10' second)");

        // over window
        Table overResult = eventTimetable.window(Over.partitionBy("id").orderBy("rt").preceding("2.rows").as("ow"))
                .select("id, rt, id.count over ow, temp.avg over ow");
        Table overSqlResult = tableEnv.sqlQuery("select id, rt, count(id) over ow, avg(temp) over ow from sensor " +
                "window ow as (partition by id order by rt rows between 2 preceding and current row)");


//        eventTimetable.printSchema();
        tableEnv.toRetractStream(overResult, Row.class).print("result");
        tableEnv.toRetractStream(overSqlResult, Row.class).print("sql");

        env.execute();
    }
}
