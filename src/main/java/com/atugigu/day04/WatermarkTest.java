package com.atugigu.day04;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class WatermarkTest {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置任务并行数量
        env.setParallelism(1);

        //todo 设置发送水位线间隔
        //env.getConfig().setAutoWatermarkInterval(60*1000L);

        //todo 引入数据源
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 9999);

        stream
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        //todo 转成毫秒时间戳
                        // ("a",1000L)
                        // [0,5)左闭右开
                        return Tuple2.of(arr[0],Long.parseLong(arr[1])*1000L);
                    }
                })
                //todo 插入水位线
                // 乱序数据流BoundedOutOfOrderness
                // 单调递增时间戳MonoousTimestaps
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                        //todo 告诉Flink的事件中的时间戳是哪一个
                        return element.f1;
                    }
                }))
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        long count = 0L;
                        for (Tuple2<String, Long> element : elements) {
                            count += 1L;
                        }

                        String start = new Timestamp(context.window().getStart()).toString();
                        String end = new Timestamp(context.window().getEnd()).toString();

                        out.collect("窗口:"+start+"---"+end+"中有"+count+"条元素");
                    }
                })
                .print();

        //todo 执行
        env.execute();
    }
}
