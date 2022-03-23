package com.soul.apitest.source;

import com.soul.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class SourceTest4_UDF {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());
        dataStream.print();
        env.execute();
    }

    static class MySensorSource implements SourceFunction<SensorReading> {

        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            Random random = new Random();

            // 设置10个传感器的初始温度
            Map<String, Double> sensorTempMap = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                sensorTempMap.put("sensor_" + (i + 1), random.nextGaussian() * 20 + 60); // 高斯随机数 正态分布 (-3,3)
            }

            while (running) {
                for (String sensorId : sensorTempMap.keySet()) {
                    Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId, newTemp);
                    sourceContext.collect(new SensorReading(sensorId, System.currentTimeMillis(), newTemp));
                }
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
