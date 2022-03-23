package com.soul.apitest.window;

import com.soul.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowTest2_CountWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("172.19.231.151", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 计数窗口 窗口大小：2 4 5 5 5 5 5
        DataStream<Double> resultStream = dataStream.keyBy("id").countWindow(5, 2)
                .aggregate(new AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double>() {

                    @Override
                    public Tuple2<Double, Integer> createAccumulator() {
                        return new Tuple2<>(0.0, 0);
                    }

                    @Override
                    public Tuple2<Double, Integer> add(SensorReading sensorReading, Tuple2<Double, Integer> doubleIntegerTuple2) {
                        return new Tuple2<>(doubleIntegerTuple2.f0 + sensorReading.getTemperature(), doubleIntegerTuple2.f1 + 1);
                    }

                    @Override
                    public Double getResult(Tuple2<Double, Integer> doubleIntegerTuple2) {
                        return doubleIntegerTuple2.f0 / doubleIntegerTuple2.f1;
                    }

                    @Override
                    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> doubleIntegerTuple2, Tuple2<Double, Integer> acc1) {
                        return new Tuple2<>(doubleIntegerTuple2.f0 + acc1.f0, doubleIntegerTuple2.f1 + acc1.f1);
                    }
                });
        resultStream.print();
        env.execute();
    }
}
