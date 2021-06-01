package com.atugigu.day07;

import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaConsumerExample {

    public static void main(String[] args) throws Exception {

        //todo 创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置任务并行数量
        env.setParallelism(1);
        //todo 配置参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty("group.id","consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset","latest");

        env.addSource(new FlinkKafkaConsumer<String>(
                "atguigu200921",
                new SimpleStringSchema(),
                properties
        ))
                .print();

        //todo 执行
        env.execute("KafkaConsumer");
    }
}
