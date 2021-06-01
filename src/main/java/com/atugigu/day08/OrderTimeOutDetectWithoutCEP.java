package com.atugigu.day08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class OrderTimeOutDetectWithoutCEP {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置并行任务数量
        env.setParallelism(1);
        //todo 引入数据源
        env.fromElements(
                Tuple3.of("user_1","create",1000L),
                Tuple3.of("user_2","create",2000L),
                Tuple3.of("user_1","pay",3000L)
        )
                //todo 200毫秒插入一次水位线
                // 插入时间戳和水位线
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                })
                )
                .keyBy(r -> r.f0)
                .process(new KeyedProcessFunction<String, Tuple3<String, String, Long>, String>() {

                    //todo 定义订单状态变量
                    private ValueState<Tuple3<String,String,Long>> orderState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        orderState = getRuntimeContext().getState(
                                new ValueStateDescriptor<Tuple3<String, String, Long>>("order-state", Types.TUPLE(Types.STRING,Types.STRING,Types.LONG))
                        );
                    }

                    @Override
                    public void processElement(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {

                           if (value.f1.equals("create")){
                               //todo 说明pay没有在create之前到来
                               if (orderState.value() == null){
                                   ctx.timerService().registerEventTimeTimer(value.f2+5*1000L);
                                   orderState.update(value);
                               }
                           }else if (value.f1.equals("pay")){

                               out.collect("订单ID："+value.f0+"支付成功");
                               orderState.update(value);
                           }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        if (orderState.value() != null&& orderState.value().f1.equals("create")){
                            out.collect("订单ID："+ctx.getCurrentKey()+"支付超时！");
                            orderState.clear();
                        }

                    }
                })
                .print();


        //todo 执行
        env.execute("OrderTimeOutDetectWithOutCEP");
    }
}
