package com.atugigu.day04;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class WaterMarkOnTimer {
    public static void main(String[] args) throws Exception {


        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        DataStreamSource<String> stream1 = env.socketTextStream("hadoop102", 9999);

        //todo 数据清洗ETL
        SingleOutputStreamOperator<Tuple2<String, Long>> streamWithWaterMark = stream1
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        })
                );
        streamWithWaterMark
                .keyBy(r -> r.f0)
                .process(new MyKeyed())
                .print();


        //todo 执行
        env.execute("WaterMarkOnTimer");
    }

    public static class MyKeyed extends KeyedProcessFunction<String,Tuple2<String,Long>,String> {

        @Override
        public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
            out.collect("数据来了！");
            //todo 注册定时器
            ctx.timerService().registerEventTimeTimer(value.f1+10*1000L);
            System.out.println("当前的水位线是"+ctx.timerService().currentWatermark());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("定时器触发了！");
        }
    }
}
