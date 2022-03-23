package com.soul.apitest.state;

import com.soul.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

public class StateTest2_KeyedState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        DataStream<Integer> resultStream = dataStream.keyBy("id").map(new RichMapFunction<SensorReading, Integer>() {

            private ValueState<Integer> keyCountState;
            private ListState<String> listState;
            private MapState<String, Integer> mapState;
            private ReducingState<SensorReading> readingReducingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                this.keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<>("value-count", Integer.class, 0));
                this.listState = getRuntimeContext().getListState(new ListStateDescriptor<>("list-count", String.class));
                this.mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>("map-count", String.class, Integer.class));
                this.readingReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>("reduce-count", new ReduceFunction<SensorReading>() {
                    @Override
                    public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {
                        return null;
                    }
                }, SensorReading.class));
            }

            @Override
            public Integer map(SensorReading s) throws Exception {
                // ValueState
                Integer count = keyCountState.value() + 1;
                keyCountState.update(count);

                // ListState
                Iterable<String> strings = this.listState.get();
                this.listState.add("hello");

                // MapState
                this.mapState.put("2", 2);
                this.mapState.get("2");
                this.mapState.remove("2");

                // ReduceState
                this.readingReducingState.clear();

                return count;
            }
        });

        resultStream.print();
        env.execute();
    }
}
