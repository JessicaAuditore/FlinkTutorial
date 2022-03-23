package com.soul.apitest.tramsform;

import com.soul.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest5_RichFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        DataStream<Tuple2<String, Integer>> resultStream = dataStream.map(new RichMapFunction<SensorReading, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), getRuntimeContext().getIndexOfThisSubtask());
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化工作 如建立数据库链接 占用多少个slot就会执行多少次
                System.out.println("open");
            }

            @Override
            public void close() throws Exception {
                // 关闭连接 情况状态
                System.out.println("close");
            }
        });
        resultStream.print();

        env.execute();
    }
}
