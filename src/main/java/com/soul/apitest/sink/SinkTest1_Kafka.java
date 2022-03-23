package com.soul.apitest.sink;


import com.soul.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class SinkTest1_Kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.50.16:9091,192.168.50.16:9092,192.168.50.16:9093");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties));

        dataStream.addSink(new FlinkKafkaProducer<String>("192.168.50.16:9091,192.168.50.16:9092,192.168.50.16:9093", "sinkTest", new SimpleStringSchema()));
        env.execute();
    }
}
