package com.atugigu.day03;

import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MaxMinTempPerWindow {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        SingleOutputStreamOperator<SensorReading> stream = env.addSource(new SensorSource()).filter(r -> r.id.equals("sensor_1"));

        //todo 分流开窗聚合
        stream
                .keyBy(r -> r.id)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new Win())
                .print();

        //todo 执行
        env.execute("MaxMinTemp");
    }
    public  static class Win extends ProcessWindowFunction<SensorReading,MaxMinTemp,String, TimeWindow> {
        private Double min = Double.MAX_VALUE;
        private Double max = Double.MIN_VALUE;
        @Override
        public void process(String key, Context context, Iterable<SensorReading> elements, Collector<MaxMinTemp> out) throws Exception {
            for (SensorReading t : elements) {
                if (t.temperature > max){
                    max = t.temperature;
                }
                if (t.temperature < min){
                    min = t.temperature;
                }
            }
            final long start = context.window().getStart();
            final long end = context.window().getEnd();
            out.collect(new MaxMinTemp(key,min,max,start,end));
        }
    }
}
