package com.soul.apitest.window;

import com.soul.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class WindowTest3_EventTimeWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        // 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> inputStream = env.socketTextStream("192.168.50.16", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
                })
                // 提取时间戳，生产watermark
                // 升序数据 设置事件时间戳和watermark
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
//                    @Override
//                    public long extractAscendingTimestamp(SensorReading sensorReading) {
//                        return sensorReading.getTimestamp() * 1000L;
//                    }
//                })
                // 乱序数据 设置事件时间戳和watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    // 有界乱序时间戳提取器 最多乱序2秒
                    @Override
                    public long extractTimestamp(SensorReading sensorReading) {
                        return sensorReading.getTimestamp() * 1000L;
                    }
                });

        // 定义侧数据流
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late"){};

        // 基于事件时间的开窗聚合，统计15s内温度的最小值
        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream.keyBy("id")
                // 定义了15s间隔的滚动eventTime事件窗口
                .timeWindow(Time.seconds(15))
                // 允许迟到数据 每15秒一个窗口，到15秒时输出一个结果，如果迟到数据在1分钟之内来了，来一个更新一次结果，1分钟之后彻底关闭窗口
                .allowedLateness(Time.minutes(1))
                // 彻底关闭窗口之后再来的数据由侧数据流接受
                .sideOutputLateData(outputTag)
                .minBy("temperature");

        minTempStream.print("minTemp");
        // 输出彻底关闭窗口之后来的数据
        minTempStream.getSideOutput(outputTag).print("late");

        env.execute();
    }
}
