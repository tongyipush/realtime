package com.atugigu.day02;

import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FilterExample {
    public static void main(String[] args) throws Exception {

        //todo 获取运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        DataStreamSource<SensorReading> readings = env.addSource(new SensorSource());

        //todo 数据进行过滤
        readings
                .filter(r -> r.id.equals("sensor_1"))
                .print();

        readings
                .filter(new FilterFunction<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading value) throws Exception {
                        return value.id.equals("sensor_1");
                    }
                })
                .print();

        readings
                .filter(new MyFilter())
                .print();

        readings
                .flatMap(new FlatMapFunction<SensorReading, SensorReading>() {
                    @Override
                    public void flatMap(SensorReading value, Collector<SensorReading> out) throws Exception {
                        //todo out收集，向下游发送数据
                        if (value.id.equals("sensor_1")){
                            out.collect(value);
                        }

                    }
                })
                .print();



        //todo 执行任务
        env.execute("filter example");
    }

    public static class MyFilter implements FilterFunction<SensorReading>{

        @Override
        public boolean filter(SensorReading value) throws Exception {
            return value.id.equals("sensor_1");
        }
    }
}
