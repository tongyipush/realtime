package com.atugigu.day09;

import com.atugigu.day05.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;
public class TopNSql {
    public static void main(String[] args) throws Exception {

        //todo 创建和运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置任务并行数量
        env.setParallelism(1);
        //todo 创建Table配置及运行环境

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        //todo 引入数据源
        DataStreamSource<String> stream = env.readTextFile("D:\\Code\\flink\\tmp\\UserBehavior.csv");
        //todo 数据清洗
        SingleOutputStreamOperator<UserBehavior> streamWithWaterMark = stream
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
                        }));

        //todo SQL
        //todo 创建临时视图
        // 不能使用timestamp，其为关键字
        tEnv.createTemporaryView("t",streamWithWaterMark,$("itemId"),$("timeStamp").rowtime().as("ts"));

        //todo 查询
        // 需求每个窗口，每个商品ITEM的访问量
        String innerSQL="SELECT itemId, count(itemId) as itemCount, HOP_END(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) as windowEnd FROM t GROUP BY itemId, HOP(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR)";
        String midSQL="SELECT itemId, itemCount, windowEnd, ROW_NUMBER() OVER(PARTITION BY windowEnd ORDER BY itemCount DESC) as row_num FROM"+"("+innerSQL+")";
        String outerSQL="SELECT itemId, itemCount, windowEnd, row_num FROM"+"("+midSQL+")"+" WHERE row_num <= 3";
        Table tableResult = tEnv.sqlQuery(outerSQL);
        tEnv.toRetractStream(tableResult, Row.class).print();


        //todo 执行
        env.execute("TopNSql");
    }
}
