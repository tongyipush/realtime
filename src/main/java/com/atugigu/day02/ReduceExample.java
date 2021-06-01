package com.atugigu.day02;

import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceExample {
    public static void main(String[] args) throws Exception {
        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        SingleOutputStreamOperator<SensorReading> readings = env.addSource(new SensorSource()).filter(r -> r.id.equals("sensor_1"));


        readings
                .keyBy(r -> r.id)
                .reduce(new ReduceFunction<SensorReading>() {
                    @Override
                    public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                        if (value1.temperature > value2.temperature){
                            return value1;
                        }else {
                            return value2;
                        }
                    }
                })
                .print();

        env.execute("reduce example");
    }
}
