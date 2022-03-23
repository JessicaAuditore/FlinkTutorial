package com.soul.apitest.processfunction;

import com.soul.apitest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ProcessTest1_KeyedProcessFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("192.168.50.16", 7777);
        DataStream<SensorReading> dataStream = inputStream.global().map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // KeyedProcessFunction有富函数的所有功能
        dataStream.keyBy("id").process(new KeyedProcessFunction<Tuple, SensorReading, Integer>() {

            private ValueState<Long> tsTimer;

            @Override
            public void open(Configuration parameters) throws Exception {
                this.tsTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("ts-timer", Long.class));
            }

            @Override
            public void processElement(SensorReading value, KeyedProcessFunction<Tuple, SensorReading, Integer>.Context ctx, Collector<Integer> collector) throws Exception {
                ctx.getCurrentKey();
                ctx.timestamp();
//                ctx.output(new OutputTag<SensorReading>("late"));
                ctx.timerService().currentProcessingTime();
                ctx.timerService().currentWatermark();
                // 闹钟
                ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1000L); // 1970年的绝对时间
                this.tsTimer.update(ctx.timerService().currentProcessingTime() + 1000L);
//                ctx.timerService().registerEventTimeTimer(value.getTimestamp());
//                ctx.timerService().deleteProcessingTimeTimer(this.tsTimer.value());
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<Tuple, SensorReading, Integer>.OnTimerContext ctx, Collector<Integer> out) throws Exception {
                System.out.println(timestamp + "定时器触发");

                ctx.getCurrentKey();
//                ctx.output(); // 侧输出流
                ctx.timeDomain();
            }
        }).print();


        env.execute();
    }
}
