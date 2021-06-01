package com.atugigu.day02;

import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AvgTempPerWindow {
    public static void main(String[] args) throws Exception {

        //todo 获取运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 创建任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        SingleOutputStreamOperator<SensorReading> stream = env.addSource(new SensorSource()).filter(r -> r.id.equals("sensor_1"));

        stream
                .keyBy(r -> r.id)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new Avg())
                .print();

        //todo 执行
        env.execute("avg temp");

    }

    public static class Avg implements AggregateFunction<SensorReading, Tuple3<String,Long,Double>, Tuple2<String,Double>> {

        @Override
        public Tuple3<String, Long, Double> createAccumulator() {
            return Tuple3.of("",0L,0.0);
        }

        @Override
        public Tuple3<String, Long, Double> add(SensorReading value, Tuple3<String, Long, Double> accumulator) {
            return Tuple3.of(value.id,accumulator.f1+1L,accumulator.f2+value.temperature);
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple3<String, Long, Double> accumulator) {
            return Tuple2.of(accumulator.f0,accumulator.f2 / accumulator.f1);
        }

        @Override
        public Tuple3<String, Long, Double> merge(Tuple3<String, Long, Double> a, Tuple3<String, Long, Double> b) {
            return null;
        }
    }
}
