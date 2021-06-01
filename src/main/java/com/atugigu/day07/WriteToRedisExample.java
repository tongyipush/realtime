package com.atugigu.day07;

import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;



public class WriteToRedisExample {
    public static void main(String[] args) throws Exception{

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        //todo 配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").build();

        //todo 将数据写入redis
        stream.addSink(new RedisSink<SensorReading>(conf,new MySinkFunction()));


        //todo 执行
        env.execute("WriteToRedisExample");

    }

    public static class MySinkFunction implements RedisMapper<SensorReading> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"event");
        }

        @Override
        public String getKeyFromData(SensorReading data) {
            return data.id;
        }

        @Override
        public String getValueFromData(SensorReading data) {
            return data.temperature+"";
        }
    }
}
