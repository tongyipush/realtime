package com.atugigu.day07;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaProducerExample {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置任务并行数量
        env.setParallelism(1);
        //todo 引入数据源
        DataStreamSource<String> stream = env.fromElements("Hello World");
        //todo 设置kafka配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092");

        stream
                .addSink(new FlinkKafkaProducer<String>(
                        "atguigu200921",
                        new SimpleStringSchema(),
                        properties
                ));

        //todo 执行
        env.execute("KafkaProducerExample");
    }
}
