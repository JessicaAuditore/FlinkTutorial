package com.soul.apitest.tramsform;

import com.soul.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest6_Partition {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        DataStream<String> dataStream1 = inputStream.shuffle();
        DataStream<String> dataStream2 = inputStream.keyBy(0);
        DataStream<String> dataStream3 = inputStream.rebalance();
        DataStream<String> dataStream4 = inputStream.global();

        env.execute();
    }
}
