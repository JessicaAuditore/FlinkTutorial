package com.soul.apitest.processfunction;

import com.soul.apitest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class ProcessTest2_ApplicationCase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> inputStream = env.socketTextStream("192.168.50.16", 7777);
        DataStream<SensorReading> dataStream = inputStream.global().map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            // 有界乱序时间戳提取器 最多乱序2秒
            @Override
            public long extractTimestamp(SensorReading sensorReading) {
                return sensorReading.getTimestamp() * 1000L;
            }
        });
        ;

        // KeyedProcessFunction有富函数的所有功能
        dataStream.keyBy("id").process(new TempConsIncreaseWarning(10)).print();

        env.execute();
    }

    static class TempConsIncreaseWarning extends KeyedProcessFunction<Tuple, SensorReading, String> {

        private Integer interval;

        public TempConsIncreaseWarning(Integer interval) {
            this.interval = interval;
        }

        // 定义状态，保存上一次的温度值，定时器时间戳
        private ValueState<Double> lastTempState;
        private ValueState<Long> timeTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class, Double.MIN_VALUE));
            this.timeTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(SensorReading sensorReading, KeyedProcessFunction<Tuple, SensorReading, String>.Context context, Collector<String> collector) throws Exception {
            Double lastTemp = this.lastTempState.value();
            Long timerTs = this.timeTsState.value();

            if (sensorReading.getTemperature() > lastTemp && timerTs == null) {
                // 如果温度第一次上升（timerTs为null）且没有定时器，注册10秒后的定时器，开始等待
                // 计算定时器时间戳
                long ts = context.timerService().currentWatermark() + interval * 1000L;
                System.out.println("传感器" + context.getCurrentKey().getField(0) + "设置" + ts + "定时器");
                context.timerService().registerEventTimeTimer(ts);
                this.timeTsState.update(ts);
            } else if (sensorReading.getTemperature() <= lastTemp && timerTs != null) {
                // 如果温度下降且有定时器
                System.out.println("传感器" + context.getCurrentKey().getField(0) + "删除定时器");
                context.timerService().deleteEventTimeTimer(timerTs);
                this.timeTsState.clear();
            }

            lastTempState.update(sensorReading.getTemperature());
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple, SensorReading, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            System.out.println("传感器" + ctx.getCurrentKey().getField(0) + "在" + this.interval + "秒之内温度连续上升");
            this.timeTsState.clear();
        }

        @Override
        public void close() throws Exception {
            this.lastTempState.clear();
        }
    }
}
