package com.atugigu.day02;

import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class MapStream {
    public static void main(String[] args) throws Exception{
        //todo 获取运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 创建并行任务数量
        env.setParallelism(1);

        //todo 引入数据源
        DataStreamSource<SensorReading> readings = env.addSource(new SensorSource());

        //todo map
        readings
                .map(r -> r.id)
                .print();
        readings
                .map(new MapFunction<SensorReading, String>() {
                    @Override
                    public String map(SensorReading value) throws Exception {
                        return value.id;
                    }
                })
                .print();

        readings
                .map(new MyMap())
                .print();

        //todo 使用flatmap实现map功能
        readings
                .flatMap(new FlatMapFunction<SensorReading, String>() {
                    @Override
                    public void flatMap(SensorReading value, Collector<String> out) throws Exception {
                        out.collect(value.id);
                    }
                })
                .print();

        env.execute("map example");






    }

    public static  class MyMap implements MapFunction<SensorReading,String>{

        @Override
        public String map(SensorReading value) throws Exception {
            return value.id;
        }
    }
}
