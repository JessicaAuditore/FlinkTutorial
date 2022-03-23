package com.soul.apitest.window;

import com.soul.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IterableUtils;
import scala.Int;

public class WindowTest1_TimeWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("172.19.231.151", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 1. 增量聚合函数
        DataStream<Integer> resultStream = dataStream.keyBy("id").timeWindow(Time.seconds(15)).aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
            @Override
            public Integer createAccumulator() {
                // 初始
                return 0;
            }

            @Override
            public Integer add(SensorReading sensorReading, Integer integer) {
                return integer + 1;
            }

            @Override
            public Integer getResult(Integer integer) {
                return integer;
            }

            @Override
            public Integer merge(Integer integer, Integer acc1) {
                return integer + acc1;
            }
        });

        // 2. 全窗口函数 相比增量函数能够拿到更多的信息 如key window
        DataStream<Tuple3<String,Long, Integer>> resultStream2 = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .apply(new WindowFunction<SensorReading, Tuple3<String,Long, Integer>, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> iterable, Collector<Tuple3<String,Long, Integer>> collector) throws Exception {
                /*
                * tuple key
                * iterable 输入
                * collector 输出
                * */
                String id = tuple.getField(0);
                Long windowEnd = timeWindow.getEnd();
                Integer count = IteratorUtils.toList(iterable.iterator()).size();
                collector.collect(new Tuple3<>(id, windowEnd, count));
            }
        });

//        resultStream.print();
        resultStream2.print();
        env.execute();
    }
}
