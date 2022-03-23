package com.soul.apitest.tramsform;

import com.soul.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformTest2_RollingAggregatom {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 保证顺序输出
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map( line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
//        KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(SensorReading::getId); //方法引用

        DataStream<SensorReading> resultStream = keyedStream.maxBy("temperature");
        resultStream.print();

        env.execute();
    }
}
