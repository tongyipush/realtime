package com.atugigu.day02;

import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


public class AggregationExample {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 创建任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        SingleOutputStreamOperator<SensorReading> stream = env.addSource(new SensorSource()).filter(r -> r.id.equals("sensor_1"));

        //todo 求最大温度值、最小温度值
        stream
                .keyBy(r -> r.id)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new MyAgr())
                .print();


        //todo 执行任务
        env.execute("aggregation");

    }

    public static  class MyAgr implements AggregateFunction<SensorReading, Tuple3<String,Double,Double>, Tuple3<String,Double,Double>> {


        @Override
        public Tuple3<String, Double, Double> createAccumulator() {
            return Tuple3.of("",Double.MAX_VALUE,Double.MIN_VALUE);
        }

        @Override
        public Tuple3<String, Double, Double> add(SensorReading value, Tuple3<String, Double, Double> accumulator) {
            return accumulator.of(value.id,Math.min(accumulator.f1,value.temperature),Math.max(accumulator.f2,value.temperature));
        }

        @Override
        public Tuple3<String, Double, Double> getResult(Tuple3<String, Double, Double> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple3<String, Double, Double> merge(Tuple3<String, Double, Double> a, Tuple3<String, Double, Double> b) {
            return null;
        }
    }
}
