package com.atugigu.day08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class DataSkew {
    public static void main(String[] args) throws Exception{

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置并行任务数量
        env.setParallelism(1);
        //todo 引入数据源
        SingleOutputStreamOperator<Tuple3<String, Long, Long>> streamAndIndexAndWaterMark = env.fromElements(
                Tuple3.of("a", 1L, 1000L),
                Tuple3.of("a", 1L, 2000L),
                Tuple3.of("a", 1L, 3000L),
                Tuple3.of("a", 1L, 4000L),
                Tuple3.of("a", 1L, 5000L),
                Tuple3.of("a", 1L, 6000L),
                Tuple3.of("a", 1L, 7000L),
                Tuple3.of("a", 1L, 8000L),
                Tuple3.of("a", 1L, 9000L),
                Tuple3.of("a", 1L, 10000L),
                Tuple3.of("b", 1L, 11000L)
        )
                .map(new MapFunction<Tuple3<String, Long, Long>, Tuple3<String, Long, Long>>() {
                    @Override
                    public Tuple3<String, Long, Long> map(Tuple3<String, Long, Long> value) throws Exception {
                        Random random = new Random();

                        return Tuple3.of(value.f0 + "-" + random.nextInt(4), value.f1, value.f2);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, Long, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        }));

        SingleOutputStreamOperator<Tuple3<String, Long, Long>> reduce1 = streamAndIndexAndWaterMark
                .keyBy(r -> r.f0)
                .reduce(new ReduceFunction<Tuple3<String, Long, Long>>() {
                    @Override
                    public Tuple3<String, Long, Long> reduce(Tuple3<String, Long, Long> value1, Tuple3<String, Long, Long> value2) throws Exception {

                        return Tuple3.of(value1.f0, value1.f1 + value2.f1, value1.f2);
                    }
                });


        SingleOutputStreamOperator<Tuple3<String, Long, Long>> reduce2 = reduce1
                .map(new MapFunction<Tuple3<String, Long, Long>, Tuple3<String, Long, Long>>() {
                    @Override
                    public Tuple3<String, Long, Long> map(Tuple3<String, Long, Long> value) throws Exception {
                        return Tuple3.of(value.f0.split("-")[0], Long.parseLong(value.f0.split("-")[1]), value.f1);
                    }
                })
                .keyBy(r -> r.f0)
                .process(new KeyedProcessFunction<String, Tuple3<String, Long, Long>, Tuple3<String, Long, Long>>() {

                    private ValueState<Long> sum0;
                    private ValueState<Long> sum1;
                    private ValueState<Long> sum2;
                    private ValueState<Long> sum3;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                         sum0 = getRuntimeContext().getState(new ValueStateDescriptor<Long>("sum0", Types.LONG));
                         sum1 = getRuntimeContext().getState(new ValueStateDescriptor<Long>("sum1", Types.LONG));
                         sum2 = getRuntimeContext().getState(new ValueStateDescriptor<Long>("sum2", Types.LONG));
                         sum3 = getRuntimeContext().getState(new ValueStateDescriptor<Long>("sum3", Types.LONG));
                    }

                    @Override
                    public void processElement(Tuple3<String, Long, Long> value, Context ctx, Collector<Tuple3<String, Long, Long>> out) throws Exception {
                        if (sum0.value() == null){
                            sum0.update(0L);
                        }
                        if (sum1.value() == null){
                            sum1.update(0L);
                        }
                        if (sum2.value() == null){
                            sum2.update(0L);
                        }
                        if (sum3.value() == null){
                            sum3.update(0L);
                        }
                        if (value.f1 == 0L){
                            sum0.update(value.f2);
                        }
                        if (value.f1 == 1L){
                            sum1.update(value.f2);
                        }
                        if (value.f1 == 2L){
                            sum2.update(value.f2);
                        }
                        if (value.f1 == 3L){
                            sum3.update(value.f2);
                        }
                        long sum = sum0.value()+sum1.value()+sum2.value()+sum3.value();

                        out.collect(Tuple3.of(value.f0,10L,sum));
                    }
                });

        reduce2.print();




        //todo 执行
        env.execute("DataSkew");
    }
}
