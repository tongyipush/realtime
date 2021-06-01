package com.atugigu.day08;

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
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class TriggerExample {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置任务并行数量
        env.setParallelism(1);
        //todo 引入数据源
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 9999);

        //todo 数据清洗+分发水位线
        SingleOutputStreamOperator<Tuple2<String, Long>> streamWithWaterMark = stream
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        }));

        streamWithWaterMark
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(new MyTrigger())
                .process(new MyProcess())
                .print();

        //todo 执行
        env.execute("triggerExample");
    }

    public static class MyTrigger extends Trigger<Tuple2<String,Long>, TimeWindow> {


        @Override
        public TriggerResult onElement(Tuple2<String, Long> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {

            //todo 每来一条数据，触发一次调用
            ValueState<Boolean> firstSeen = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("first-seen", Types.BOOLEAN));
            if (firstSeen == null){
                //todo 第一条数据到来调用
                // t是当前水位线紧接着的下一个整数秒
                long t = ctx.getCurrentWatermark() + (1000L - ctx.getCurrentWatermark() % 1000L);
                //todo 注册的定时器的逻辑在onEventTime方法
                ctx.registerEventTimeTimer(t);
                ctx.registerEventTimeTimer(window.getEnd());

                //todo 保证只有窗口中的第一条数据到来时，才会进入条件分支
                firstSeen.update(true);
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            //todo 使用的是时间时间，所以处理时间的定时器什么都不做
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long ts, TimeWindow window, TriggerContext ctx) throws Exception {

            //todo 当前水位线达到TS时，触发OnTimer函数执行
            // 定时器
            if ( ts == window.getEnd()){
                return TriggerResult.FIRE_AND_PURGE;
            }else {
                long t = ctx.getCurrentWatermark() + (1000L - ctx.getCurrentWatermark() % 1000L);
                if (t < window.getEnd()){
                    ctx.registerEventTimeTimer(t);
                }

                return TriggerResult.FIRE;
            }
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ValueState<Boolean> firstSeen = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("first-seen", Types.BOOLEAN));
            firstSeen.clear();

        }
    }

    public static class MyProcess extends ProcessWindowFunction {

        @Override
        public void process(Object o, Context context, Iterable elements, Collector out) throws Exception {

            long count = 0L;
            for (Object element : elements) {
                count += 1;
            }
            out.collect("窗口中包含:"+count+"个元素");
        }
    }
}
