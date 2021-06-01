package com.atugigu.day04;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class UpdateWindowResultWithLateEvent1 {
    public static void main(String[] args) throws Exception{

        //todo 创建数据源
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 9999);

        //todo 数据清洗
        SingleOutputStreamOperator<Tuple2<String, Long>> streamWithWaterMark = stream
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                //todo 分发水位线和时间戳
                // 产生有延迟的乱序时间戳
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        })
                );
        //todo 分流开窗聚合
        streamWithWaterMark
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //todo 允许迟到时间
                .allowedLateness(Time.seconds(5))
                .process(new UpdateWindow())
                .print();


        //todo 执行
        env.execute("UpdateWindowWithLateEvent");
    }

    public static class UpdateWindow extends ProcessWindowFunction<Tuple2<String,Long>,String,String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
            long count = 0L;
            for (Tuple2<String, Long> element : elements) {
                count += 1;
            }
            ValueState<Boolean> isUpdate = context.windowState().getState(
                    new ValueStateDescriptor<>("is-update", Types.BOOLEAN)
            );

            if (isUpdate.value() == null ){
                out.collect("窗口第一次关闭,一共有"+count+"条元素");
                isUpdate.update(true);
            } else {
                out.collect("迟到元素到达,一共有"+count +"元素");
            }
        }
    }
}
