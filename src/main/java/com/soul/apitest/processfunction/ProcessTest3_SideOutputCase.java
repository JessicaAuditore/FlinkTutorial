package com.soul.apitest.processfunction;

import com.soul.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ProcessTest3_SideOutputCase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> inputStream = env.socketTextStream("192.168.50.16", 7777);
        DataStream<SensorReading> dataStream = inputStream.global().map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("lowTemp") {};

        // 高低温分流
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream.keyBy("id").process(new ProcessFunction<SensorReading, SensorReading>() {

            @Override
            public void processElement(SensorReading sensorReading, ProcessFunction<SensorReading, SensorReading>.Context context, Collector<SensorReading> collector) throws Exception {
                if (sensorReading.getTemperature() > 30) {
                    collector.collect(sensorReading);
                } else {
                    context.output(outputTag, sensorReading);
                }
            }
        });

        highTempStream.print("high-temp");
        highTempStream.getSideOutput(outputTag).print("low-temp");

        env.execute();
    }
}
