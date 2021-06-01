package com.atugigu.day08;

import com.atugigu.day05.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
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
import java.util.Iterator;

public class UVImproved {
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
                        final String[] arr = value.split(",");
                        return new UserBehavior(arr[0],arr[1],arr[2],arr[3],Long.parseLong(arr[4]) * 1000L);
                    }
                })
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
                .aggregate(new Agg(),new WindowResult())
                .print();

        //todo 执行
        env.execute("UV");
    }

    public static class Agg implements AggregateFunction<Tuple2<String,String>, HashSet<String>,Long> {

        @Override
        public HashSet<String> createAccumulator() {
            HashSet<String> hashSet = new HashSet<String>();
            return hashSet;
        }

        @Override
        public HashSet<String> add(Tuple2<String, String> value, HashSet<String> accumulator) {
            accumulator.add(value.f1);
            return accumulator;
        }

        @Override
        public Long getResult(HashSet<String> accumulator) {
            long count = 0L;
            Iterator<String> iterator = accumulator.iterator();
            while (iterator.hasNext()){
                iterator.next();
                count += 1L;

            }
            long size =(long) accumulator.size();
            return count;
        }

        @Override
        public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
            return null;
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long,String,String, TimeWindow>{

        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {

            String start = new Timestamp(context.window().getStart()).toString();
            String end = new Timestamp(context.window().getEnd()).toString();

            out.collect("窗口:"+start+"---"+end+"uv数量为"+elements.iterator().next());
        }
    }
}
