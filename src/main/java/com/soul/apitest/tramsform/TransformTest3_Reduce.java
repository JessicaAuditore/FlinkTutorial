package com.soul.apitest.tramsform;

import com.soul.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest3_Reduce {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 保证顺序输出
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
//        KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(SensorReading::getId); //方法引用

        DataStream<SensorReading> resultStream = keyedStream.reduce((sensorReading, t1) -> {
            // 后面一个为新数据
            return new SensorReading(sensorReading.getId(), t1.getTimestamp(), Math.max(sensorReading.getTemperature(), t1.getTemperature()));
        });
        resultStream.print();

        env.execute();
    }
}
