package com.atugigu.day02;

import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowRollExample {
    public static void main(String[] args) throws Exception {
        //todo 获取运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 创建任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        SingleOutputStreamOperator<SensorReading> readings = env.addSource(new SensorSource()).filter(r -> r.id.equals("sensor_1"));

        readings
                .map(new MapFunction<SensorReading, Tuple3<String,Double,Double>>() {
                    @Override
                    public Tuple3<String, Double, Double> map(SensorReading value) throws Exception {
                        return Tuple3.of(value.id,value.temperature,value.temperature);
                    }
                })

                .keyBy(r -> r.f0)
                //todo 5s处理时间的滚动窗口
                // 分组、开窗、聚合
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple3<String, Double, Double>>() {
                    @Override
                    public Tuple3<String, Double, Double> reduce(Tuple3<String, Double, Double> value1, Tuple3<String, Double, Double> value2) throws Exception {

                        Double minTemp,maxTemp;
                        if (value1.f1 > value2.f1){
                            minTemp = value2.f1;
                        } else {
                            minTemp = value1.f1;
                        }

                        if (value1.f1 > value2.f1){
                            maxTemp = value1.f1;
                        } else {
                            maxTemp = value2.f1;
                        }
                        return Tuple3.of(value1.f0,minTemp,maxTemp);
                    }

                })
                .print();

        //todo 执行
        env.execute("TumblingProcessingTimeWindows");

    }
}
