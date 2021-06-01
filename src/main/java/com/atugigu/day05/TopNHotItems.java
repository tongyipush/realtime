package com.atugigu.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

public class TopNHotItems {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置任务并行数量
        env.setParallelism(1);
        //todo 引入数据源
        DataStreamSource<String> stream = env.readTextFile("D:\\Code\\flink\\tmp\\UserBehavior.csv");

        SingleOutputStreamOperator<ItemViewCount> itemIdCount = stream
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new UserBehavior(arr[0], arr[1], arr[2], arr[3], Long.parseLong(arr[4]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.timeStamp;
                            }
                        }))
                .keyBy(r -> r.itemId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new MyAgg(), new MyProcessWindow());

        itemIdCount
                .keyBy(r -> r.windowEnd)
                .process(new KeyedTopN(3L))
                .print();

        //todo 执行
        env.execute("TopNHotItems");
    }

    public static class MyAgg implements AggregateFunction<UserBehavior,Long,Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator+1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class MyProcessWindow extends ProcessWindowFunction<Long,ItemViewCount,String, TimeWindow> {

        @Override
        public void process(String key, Context ctx, Iterable<Long> elements, Collector<ItemViewCount> out) throws Exception {
            out.collect(new ItemViewCount(key,elements.iterator().next(),ctx.window().getStart(),ctx.window().getEnd()));

        }
    }

    public static class KeyedTopN extends KeyedProcessFunction<Long,ItemViewCount,String> {
        //todo 列表状态
        private ListState<ItemViewCount> listState;

        public Long threshold;

        public KeyedTopN(Long threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("list", Types.POJO(ItemViewCount.class))
            );
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);
            //todo 注册定时器
            // 某一个时间戳只能注册一个定时器
            // 所以这里只会在第一条数据到来的时候注册一个定时器
            ctx.timerService().registerEventTimeTimer(value.windowEnd+200L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            ArrayList<ItemViewCount> arrayList = new ArrayList<ItemViewCount>();

            for (ItemViewCount itemViewCount : listState.get()) {
                arrayList.add(itemViewCount);
            }
            //todo 清空列表状态
            listState.clear();

            //todo 定时器用来排序
            arrayList.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {

                    return o2.count.intValue()-o1.count.intValue();
                }
            });

            StringBuilder result = new StringBuilder();
            result.append("==========================\n");
            result.append("结束窗口为:");
            result.append(new Timestamp(timestamp-200L));//恢复窗口结束时间
            result.append("\n");
            for (int i=0;i<threshold;i++) {
                result
                        .append("第"+(i+1)+"名商品ID是:"+arrayList.get(i).itemId)
                        .append("\n")
                        .append("pv数量是:"+arrayList.get(i).count)
                        .append("\n");

            }

            result.append("==========================\n");
            out.collect(result.toString());
        }
    }
}
