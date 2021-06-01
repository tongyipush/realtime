package com.atugigu.day01;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadFromSensorReading {
    public static void main(String[] args) throws Exception {
        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        DataStreamSource<SensorReading> readings = env.addSource(new SensorSource());

        readings.print();

        env.execute("高斯造斯 测试");
    }
}
