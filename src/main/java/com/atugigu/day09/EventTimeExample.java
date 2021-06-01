package com.atugigu.day09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class EventTimeExample {

    //todo 订单超时 布隆过滤器 数据倾斜
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置任务并行数量
        env.setParallelism(1);
        //todo 引入数据源
        SingleOutputStreamOperator<Tuple2<String, Long>> stream = env.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        }));

        //todo 创建table配置和运行时环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        //todo 使用TableAPI
        // rowTime()指定时间为事件时间
        // 将dataStream转换成table
        Table table = tEnv.fromDataStream(stream, $("key"), $("ts").rowtime());
        Table tableResult = table
                .window(Tumble.over(lit(5).seconds()).on($("ts")).as("w"))
                .groupBy($("key"), $("w"))
                .select($("key"), $("key").count());
//        tEnv.toRetractStream(tableResult, Row.class).print();

        //todo 使用sql
        // rowTime()指定为事件时间
        tEnv.createTemporaryView("t",stream,$("key"),$("ts").rowtime());
        Table table1 = tEnv.sqlQuery("SELECT key, COUNT(key), TUMBLE_END(ts, INTERVAL '5' SECOND) FROM t GROUP BY key, TUMBLE(ts, INTERVAL '5' SECOND)");
        tEnv.toRetractStream(table1,Row.class).print();


        //todo 执行
        env.execute("EventTime");
    }
}
