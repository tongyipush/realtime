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

public class AvgTempWithFull {
    public static void main(String[] args) throws Exception{

        //todo 获取连接时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        SingleOutputStreamOperator<SensorReading> stream = env.addSource(new SensorSource()).filter(r -> r.id.equals("sensor_1"));

        stream
                .keyBy(r -> r.id)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new Agg(),new Win())
                .print();

        //todo 执行
        env.execute("AvgTempWithFull");
    }

    public static class Agg implements AggregateFunction<SensorReading, Tuple2<Long,Double>,Double>{

        @Override
        public Tuple2<Long, Double> createAccumulator() {
            return Tuple2.of(0L,0.0);
        }

        @Override
        public Tuple2<Long, Double> add(SensorReading value, Tuple2<Long, Double> accumulator) {
            return Tuple2.of(accumulator.f0+1L,accumulator.f1+value.temperature);
        }

        @Override
        public Double getResult(Tuple2<Long, Double> accumulator) {
            return accumulator.f1/accumulator.f0;
        }

        @Override
        public Tuple2<Long, Double> merge(Tuple2<Long, Double> a, Tuple2<Long, Double> b) {
            return null;
        }
    }


    public static class Win extends ProcessWindowFunction<Double,String,String, TimeWindow>{

        @Override
        public void process(String key, Context context, Iterable<Double> elements, Collector<String> out) throws Exception {
            Double avgTemp = elements.iterator().next();
            String start = new Timestamp(context.window().getStart()).toString();
            String end = new Timestamp(context.window().getEnd()).toString();
            out.collect("ID:"+key+"时间窗口:"+start+"---"+end+"平均温度:"+avgTemp);
        }
    }
}
