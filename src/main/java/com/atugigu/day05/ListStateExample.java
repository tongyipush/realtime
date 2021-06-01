package com.atugigu.day05;

import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ListStateExample {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        stream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                    @Override
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })
                )
                .keyBy(r -> r.id)
                .process(new MyListState())
                .print();

        //todo 执行
        env.execute("ListState");
    }

    public static class MyListState extends KeyedProcessFunction<String,SensorReading,String> {
        private ListState<SensorReading> listState;
        private ValueState<Long> onTimer;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("ListState", Types.POJO(SensorReading.class))
            );
            onTimer = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("onTimer",Types.LONG)
            );
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {

            listState.add(value);
            if (onTimer.value() == null){
                ctx.timerService().registerEventTimeTimer(value.timestamp+10*1000L);
                onTimer.update(value.timestamp+10*1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            long count = 0L;
            for (SensorReading o : listState.get()) {
                count += 1;
            }
            out.collect("列表里一共有"+ count +" 元素");
            onTimer.clear();
        }
    }
}
