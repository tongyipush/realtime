package com.atugigu.day08;

import com.atugigu.day05.UserBehavior;
import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;

public class UV {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置任务并行数量
        env.setParallelism(1);
        //todo 引入数据源
        env.readTextFile("D:\\Code\\flink\\tmp\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new UserBehavior(arr[0],arr[1],arr[2],arr[3],Long.parseLong(arr[4])*1000L);
                    }
                })
                //TODO 发发时间戳和水位线
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                }))
                .filter(r -> r.behaviorType.equals("pv"))
                .map(new MapFunction<UserBehavior, Tuple2<String,String>>() {
                    @Override
                    public Tuple2<String, String> map(UserBehavior value) throws Exception {

                        return Tuple2.of("key",value.userId);
                    }
                })
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new ProcessWindowFunction<Tuple2<String, String>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, String>> elements, Collector<String> out) throws Exception {
                        //todo 放入元素需要去重，需要使用hashset,底层是hashmap,value值为常量值
                        HashSet<String> set = new HashSet<>();
                        for (Tuple2<String, String> element : elements) {
                            set.add(element.f1);
                        }
                        String start = new Timestamp(context.window().getStart()).toString();
                        String end = new Timestamp(context.window().getEnd()).toString();
                        out.collect("窗口:"+start+"---"+end+"UV数量:"+set.size());

                    }
                })
                .print();

        //todo 执行
        env.execute("UV");
    }
}
