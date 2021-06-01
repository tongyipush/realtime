package com.atugigu.day03;

import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.BooleanNode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class SensorSwitch {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        KeyedStream<SensorReading, String> stream1 = env
                .addSource(new SensorSource())
                .keyBy(r -> r.id);

        KeyedStream<Tuple2<String, Long>, String> stream2 = env
                .fromElements(Tuple2.of("sensor_3", 10 * 1000L))
                .keyBy(r -> r.f0);

        stream1
                .connect(stream2)
                .process(new SwitchProcess())
                .print();

        //todo 执行
        env.execute("SensorSwitch");
    }

    public static class SwitchProcess extends CoProcessFunction<SensorReading,Tuple2<String,Long>,SensorReading>{
        //todo 状态变量，用来当开关
        private ValueState<Boolean> forwardEnabled;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //todo 获取状态，如果没有就会初始化状态
            forwardEnabled = getRuntimeContext().getState(
                    new ValueStateDescriptor<Boolean>("switch", Types.BOOLEAN)
            );
        }

        @Override
        public void processElement1(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
            //todo 处理来自第一条流的元素，也就是传感器的读数
            if (forwardEnabled.value() != null && forwardEnabled.value()){

                out.collect(value);
            }
        }

        @Override
        public void processElement2(Tuple2<String, Long> value, Context ctx, Collector<SensorReading> out) throws Exception {
            //todo 处理来自第二条流的元素，只有一个元素
            //todo 打开开关
            forwardEnabled.update(true);

            //todo 注册定时器
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+value.f1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SensorReading> out) throws Exception {
            super.onTimer(timestamp, ctx, out);

            //todo 关闭开关
            forwardEnabled.clear();
        }
    }
}
