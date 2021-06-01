package com.atugigu.day03;

import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class MinMaxTempWithFullWindow {
    public static void main(String[] args) throws Exception {

        //todo 获取运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        SingleOutputStreamOperator<SensorReading> stream = env.addSource(new SensorSource()).filter(r -> r.id.equals("sensor_1"));


        //todo 求最大最小温度，开窗时间
        stream
                .keyBy(r -> r.id)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new Agg(),new Win())
                .print();

        //todo 执行
        env.execute("MinMaxTempWithFullWindow");
    }

    public static class Agg implements AggregateFunction<SensorReading, Tuple2<Double,Double>,Tuple2<Double,Double>> {

        @Override
        public Tuple2<Double, Double> createAccumulator() {
            return Tuple2.of(Double.MAX_VALUE,Double.MIN_VALUE);
        }

        @Override
        public Tuple2<Double, Double> add(SensorReading value, Tuple2<Double, Double> accumulator) {
//            if (value.temperature < accumulator.f0){
//                accumulator.f0 = value.temperature;
//            }
//            if (value.temperature > accumulator.f1){
//                accumulator.f1 = value.temperature;
//            }
//            return accumulator;
            return Tuple2.of(Math.min(accumulator.f0,value.temperature),Math.max(accumulator.f1,value.temperature));
        }

        @Override
        public Tuple2<Double, Double> getResult(Tuple2<Double, Double> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<Double, Double> merge(Tuple2<Double, Double> a, Tuple2<Double, Double> b) {
            return null;
        }
    }

    public static class Win extends ProcessWindowFunction<Tuple2<Double,Double>,MaxMinTemp,String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Tuple2<Double, Double>> elements, Collector<MaxMinTemp> out) throws Exception {
            Tuple2<Double, Double> MaxMin = elements.iterator().next();
            long start = context.window().getStart();
            long end = context.window().getEnd();

            out.collect(new MaxMinTemp(key,MaxMin.f0,MaxMin.f1,start,end));
        }
    }
}
