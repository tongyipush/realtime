package com.atugigu.day08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Random;

public class DataSkewImproved {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置任务并行数量
        env.setParallelism(1);
        //todo 引入数据
        env.fromElements(
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
                .map(new MapFunction<Tuple3<String, Long, Long>, Tuple3<String,Long,Long>>() {
                    @Override
                    public Tuple3<String, Long, Long> map(Tuple3<String, Long, Long> value) throws Exception {
                        Random random = new Random();
                        return Tuple3.of(value.f0+"-"+random.nextInt(4),value.f1,value.f2);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                })
                )
                .keyBy(r -> r.f0)
                .process(new KeyedProcessFunction<String, Tuple3<String, Long, Long>, Tuple2<String, Long>>() {

                    private ValueState<Tuple2<String,Long>> sum;
                    private ValueState<Long> timerTS;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        sum = getRuntimeContext().getState(
                                new ValueStateDescriptor<Tuple2<String,Long>>("sum", Types.TUPLE(Types.STRING,Types.LONG))
                        );
                        timerTS = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTS",Types.LONG));
                    }

                    @Override
                    public void processElement(Tuple3<String, Long, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {

                        if (sum.value() == null){
                            sum.update(Tuple2.of(value.f0,value.f1));
                            ctx.timerService().registerEventTimeTimer(value.f2+10*1000L);
                            timerTS.update(value.f2+10*1000L);
                        }else {
                            Long cnt = sum.value().f1;
                            sum.update(Tuple2.of(sum.value().f0,cnt+value.f1));
                            if (timerTS.value() == null){
                                ctx.timerService().registerEventTimeTimer(value.f2+10*1000L);
                                timerTS.update(value.f2+10*1000L);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect(Tuple2.of(sum.value().f0,sum.value().f1));
                        timerTS.clear();
                    }
                })
                .map(new MapFunction<Tuple2<String, Long>, Tuple3<String,Integer,Long>>() {
                    @Override
                    public Tuple3<String,Integer,Long> map(Tuple2<String, Long> value) throws Exception {
                        return Tuple3.of(value.f0.split("-")[0],Integer.parseInt(value.f0.split("-")[1]),value.f1);
                    }
                })
                .keyBy(r -> r.f0)
                .process(new KeyedProcessFunction<String, Tuple3<String,Integer, Long>, Tuple2<String,Long>>() {

                    private MapState<Long,Long> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("mapState",Types.LONG,Types.LONG));
                    }

                    @Override
                    public void processElement(Tuple3<String,Integer,Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        long sum = 0L;
                        mapState.put((long)value.f1,value.f2);
                        for (Long aLong : mapState.values()) {
                            sum += aLong;
                        }
                        out.collect(Tuple2.of(value.f0,sum));
                    }
                })
                .print();

        //todo 执行
        env.execute("DataSkewImproved");
    }
}
