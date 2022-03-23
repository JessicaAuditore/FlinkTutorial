package com.soul.apitest.state;

import com.soul.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StateTest3_KeyedStateApplicationCase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个flatMap的操作，检测温度跳变，输出报警
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = dataStream.keyBy("id").flatMap(new TempChangeWarning(10.0));

        resultStream.print();
        env.execute();
    }

    public static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {
        // 温度跳变阈值
        private Double threshold;

        public TempChangeWarning(Double threshold) {
            this.threshold = threshold;
        }

        // 保存上一次的温度值
        private ValueState<Double> lastState;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.lastState = getRuntimeContext().getState(new ValueStateDescriptor<>("last-temp", Double.class));
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> collector) throws Exception {
            Double lastTemp = this.lastState.value();
            if (lastTemp != null) {
                double diff = Math.abs(sensorReading.getTemperature() - lastTemp);
                if (diff >= this.threshold) {
                    collector.collect(new Tuple3<>(sensorReading.getId(), lastTemp, sensorReading.getTemperature()));
                }
            }
            this.lastState.update(sensorReading.getTemperature());
        }
    }
}
