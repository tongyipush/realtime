package com.atugigu.day05;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class IntervalJoinExample {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设设置任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        SingleOutputStreamOperator<Tuple3<String, String, Long>> streamClick = env.fromElements(
                Tuple3.of("user_1", "click", 60 * 10 * 1000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        }));

        SingleOutputStreamOperator<Tuple3<String, String, Long>> streamBrowser = env.fromElements(
                Tuple3.of("user_1", "browser", 60 * 1000L),
                Tuple3.of("user_1", "browser", 60 * 6 * 1000L),
                Tuple3.of("user_1", "browser", 60 * 20 * 1000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        }));
//        streamClick
//                .keyBy(r -> r.f0)
//                .intervalJoin(streamBrowser.keyBy(r -> r.f0))
//                .between(Time.minutes(-10),Time.minutes(0))
//                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
//                    @Override
//                    public void processElement(Tuple3<String, String, Long> left, Tuple3<String, String, Long> right, Context ctx, Collector<String> out) throws Exception {
//                        out.collect(left+"---"+right);
//                    }
//                })
//                .print();

        streamBrowser
                .keyBy(r -> r.f0)
                .intervalJoin(streamClick.keyBy(r -> r.f0))
                .between(Time.minutes(-10),Time.minutes(10))
                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    @Override


                    public void processElement(Tuple3<String, String, Long> left, Tuple3<String, String, Long> right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(left+"--->"+right);
                    }
                })
                .print();


        //todo 执行
        env.execute("IntervalJoin");

    }
}
