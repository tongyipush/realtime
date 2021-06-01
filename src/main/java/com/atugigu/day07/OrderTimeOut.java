package com.atugigu.day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import java.util.List;
import java.util.Map;

public class OrderTimeOut {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        SingleOutputStreamOperator<Tuple3<String, String, Long>> streamWithWaterMark = env.fromElements(
                Tuple3.of("user_1", "create", 1000L),
                Tuple3.of("user_2", "create", 2000L),
                Tuple3.of("user_1", "pay", 3000L)
        )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        }));

        //todo 设置匹配模板
        Pattern<Tuple3<String, String, Long>, Tuple3<String, String, Long>> pattern = Pattern
                .<Tuple3<String, String, Long>>begin("create")
                .where(new SimpleCondition<Tuple3<String, String, Long>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Long> value) throws Exception {
                        return value.f1.equals("create");
                    }
                })
                .next("pay")
                .where(new SimpleCondition<Tuple3<String, String, Long>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Long> value) throws Exception {
                        return value.f1.equals("pay");
                    }
                })
                .within(Time.seconds(5));

        //todo CEP库
        // 第一个测输出标签，用来发送支付超时的订单ID
        // 第二个参数：用来处理支付超时的类
        // 第三个参数：用来处理支付正常完成的订单的类
        SingleOutputStreamOperator<Tuple3<String, String, Long>> result = CEP
                .pattern(streamWithWaterMark.keyBy(r -> r.f0), pattern)
                .flatSelect(
                        new OutputTag<Tuple3<String, String, Long>>("create-only") {
                        },
                        new PatternFlatTimeoutFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>>() {
                                @Override
                            public void timeout(Map<String, List<Tuple3<String, String, Long>>> pattern, long timeoutTimestamp, Collector<Tuple3<String, String, Long>> out) throws Exception {

                                out.collect(pattern.get("create").get(0));
                                System.out.println("订单超时的ID是" + pattern.get("create").get(0).f0);
                            }
                        }, new PatternFlatSelectFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>>() {
                            @Override
                            public void flatSelect(Map<String, List<Tuple3<String, String, Long>>> pattern, Collector<Tuple3<String, String, Long>> out) throws Exception {

                                Tuple3<String, String, Long> pay = pattern.get("pay").get(0);
                                out.collect(pay);
                                System.out.println("支付成功的ID是" + pay.f0);

                            }
                        }
                );

        //todo 主流
        result.print();
        //todo 测输出流
        result.getSideOutput(new OutputTag<Tuple3<String, String, Long>>("create-only") {
        }).print();


        //todo 执行
        env.execute("");
    }
}
