package com.atugigu.day02;

import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionExample {
    public static void main(String[] args) throws Exception {
        //todo 创建运行时环境
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

         //todo 设置并行任务数量
        env.setParallelism(1);

        //todo 引入数据源
         SingleOutputStreamOperator<SensorReading> sensor_1 = env.addSource(new SensorSource()).filter(r -> r.id.equals("sensor_1"));
         SingleOutputStreamOperator<SensorReading> sensor_2 = env.addSource(new SensorSource()).filter(r -> r.id.equals("sensor_2"));
         SingleOutputStreamOperator<SensorReading> sensor_3 = env.addSource(new SensorSource()).filter(r -> r.id.equals("sensor_3"));

        //todo union与connect的区别
        // union的类型必须一样，但是不限制流个数
        // connect只能合并两条流，元素类型可以不同
         DataStream<SensorReading> union = sensor_1
                .union(sensor_2, sensor_3);

         union.print();

         env.execute("union example");
    }

}
