package com.atugigu.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class KeyedProcessF {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 9999);

        //todo a 1
        stream
                .map(new MapFunction<String, Tuple2<String,String>>() {
                    @Override
                    public Tuple2<String, String> map(String value) throws Exception {
                        String[] s = value.split(" ");
                        return  Tuple2.of(s[0],s[1]);
                    }
                })
                .keyBy(r -> r.f0)
                .process(new keyed())
                .print();
        //todo 执行
        env.execute("KeyedProcess");
    }

    public static class keyed extends KeyedProcessFunction<String,Tuple2<String,String>,String> {

        @Override
        public void processElement(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {

            //todo 获取当前机器时间
            long curTime = ctx.timerService().currentProcessingTime();

            //todo 获取10s之后的时间
            long tenAfterCur = curTime + 10 * 1000L;

            //todo 注册定时器
            ctx.timerService().registerProcessingTimeTimer(tenAfterCur);

            out.collect("数据来了，当前时间戳是:"+new Timestamp(curTime));

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("定时器启动了"+" key为:"+ctx.getCurrentKey()+"当前时间:"+new Timestamp(timestamp));
        }
    }
}
