package com.soul.apitest.state;

import com.soul.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StateTest4_FaultTolerance {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        // 状态后端
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend(""));
        env.setStateBackend(new RocksDBStateBackend(""));

        // checkpoint配置
        env.getCheckpointConfig().setCheckpointInterval(300); // 检查点间隔
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 精确一次
        env.getCheckpointConfig().setCheckpointTimeout(60000L); // 状态后端保存状态的超时时间
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2); // 能同时处理的保存点个数
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L); // 前一次checkpoint保存结束到下一次checkpoint保存开始之间的最小间隔
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true); // 倾向于用checkpoint而不是savepoint恢复
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0); // 容忍checkpoint保存失败次数

        // 故障重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L)); // 固定延迟重启
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1))); // 一段时间内失败次数上限

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.print();
        env.execute();
    }
}
