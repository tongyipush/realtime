package com.atugigu.day09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class TwoStreamJoin {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置并行任务数量
        env.setParallelism(1);
        //todo 引入数据源
        SingleOutputStreamOperator<Tuple3<String, String, Long>> order_stream = env.fromElements(
                Tuple3.of("order_1", "pay", 1000L),
                Tuple3.of("order_2", "pay", 2000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        })
                );

        SingleOutputStreamOperator<Tuple3<String, String, Long>> pay_stream = env.fromElements(
                Tuple3.of("order_1", "weixin", 3000L),
                Tuple3.of("order_3", "weixin", 4000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        })
                );

        order_stream
                .keyBy(r -> r.f0)
                .connect(pay_stream.keyBy(r -> r.f0))
                .process(new TwoJoin())
                .print();


        //todo 执行
        env.execute("TwoStreamJoin");
    }

    public static class TwoJoin extends CoProcessFunction<Tuple3<String,String,Long>,Tuple3<String,String,Long>,String>{

        private ValueState<Tuple3<String,String,Long>> orderState;
        private ValueState<Tuple3<String,String,Long>> payState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            orderState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String, String, Long>>("order-state", Types.TUPLE(Types.STRING,Types.STRING,Types.LONG)));
            payState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String, String, Long>>("pay-state", Types.TUPLE(Types.STRING,Types.STRING,Types.LONG)));
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            if (payState.value() != null){
                out.collect("订单ID:"+value.f0+"支付成功!");
                payState.clear();
            }else {
                orderState.update(value);
                ctx.timerService().registerEventTimeTimer(value.f2+5000L);
            }

        }

        @Override
        public void processElement2(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            if ( orderState.value() != null){

                out.collect("订单ID:"+value.f0+"支付成功!");
                orderState.clear();
            }else {

                payState.update(value);
                ctx.timerService().registerEventTimeTimer(value.f2+5000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            if (orderState.value() != null){
                out.collect("订单ID:"+ orderState.value().f0+"支付失败!");
                orderState.clear();
            }
            if (payState.value() != null){
                out.collect("订单ID:"+payState.value().f0+"支付失败!");
                payState.clear();
            }
        }
    }
}
