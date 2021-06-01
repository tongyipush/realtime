package com.atugigu.day08;

import com.atugigu.day05.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.curator4.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.curator4.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class UVBloomFilter {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置并行任务数量
        env.setParallelism(1);
        //todo 引入数据源
        DataStreamSource<String> stream = env.readTextFile("D:\\Code\\flink\\tmp\\UserBehavior.csv");

        //todo 数据清洗
        stream
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new UserBehavior(arr[0],arr[1],arr[2],arr[3],Long.parseLong(arr[4]) * 1000L);
                    }
                })
                .filter(r -> r.behaviorType.equals("pv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                }))
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
        env.execute("UVBloom");
    }

    public static class Agg implements AggregateFunction<Tuple2<String,String>, Tuple2<BloomFilter<Long>,Long>,Long> {

        //todo 创建累加器
        @Override
        public Tuple2<BloomFilter<Long>, Long> createAccumulator() {
            //todo 假设独立用户数是100万，误判率是0.01
            return Tuple2.of(BloomFilter.create(Funnels.longFunnel(),100000,0.01),0L);
        }

        //todo 定义累加器计算规则
        @Override
        public Tuple2<BloomFilter<Long>, Long> add(Tuple2<String, String> value, Tuple2<BloomFilter<Long>, Long> accumulator) {
            if (!accumulator.f0.mightContain(Long.parseLong(value.f1))){
                //todo 如果userID没来过，那么执行put操作
                accumulator.f0.put(Long.parseLong(value.f1));
                accumulator.f1 += 1L; //todo UV数量加一
                return accumulator;
            }
            return accumulator;
        }

        @Override
        public Long getResult(Tuple2<BloomFilter<Long>, Long> accumulator) {
            return accumulator.f1;
        }

        @Override
        public Tuple2<BloomFilter<Long>, Long> merge(Tuple2<BloomFilter<Long>, Long> a, Tuple2<BloomFilter<Long>, Long> b) {
            return null;
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long,String,String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {

            long start = context.window().getStart();
            long end = context.window().getEnd();
            out.collect("窗口"+start+"---"+end+"UV数量为:"+elements.iterator().next());
        }
    }
}
