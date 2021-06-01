package com.atugigu.day02;

import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> readings = env.addSource(new SensorSource());

        KeyedStream<SensorReading, String> keyedReadings = readings.keyBy(r -> r.id);

        //todo 每个keyby分区维护一个累加器，来一个值跟累加器的值进行比较更新
        keyedReadings.max("temperature").print();

        env.execute("keyby example");
    }
}
