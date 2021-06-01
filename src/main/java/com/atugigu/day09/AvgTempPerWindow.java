package com.atugigu.day09;

import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class AvgTempPerWindow {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置并行任务数量
        env.setParallelism(1);

        //todo 创建table运行时环境及配置
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        //todo 引入数据源
        SingleOutputStreamOperator<SensorReading> stream = env.addSource(new SensorSource()).filter(r -> r.id.equals("sensor_1"));
        //todo 将DataStream转换为Table
        // 添加处理时间字段
        Table table = tEnv.fromDataStream(stream, $("id"), $("temperature").as("temp"), $("timestamp").as("ts"), $("ptime").proctime());
        //todo 查询
        Table tableResult = table
                .window(Tumble.over(lit(10).seconds()).on($("ptime")).as("w"))
                .groupBy($("id"), $("w"))
                .select($("id"), $("temp").avg(), $("w").end());

//        tEnv.toRetractStream(tableResult, Row.class).print();


        //todo SQL
        tEnv.createTemporaryView("sensor",stream,$("id"), $("temperature").as("temp"), $("timestamp").as("ts"), $("ptime").proctime());

        Table tableResultSql = tEnv.sqlQuery("SELECT id, avg(temp), TUMBLE_START(ptime, INTERVAL '10' SECOND), TUMBLE_END(ptime, INTERVAL '10' SECOND) FROM sensor GROUP BY id, TUMBLE(ptime, INTERVAL '10' SECOND)");
        tEnv.toRetractStream(tableResultSql,Row.class).print();
        //todo 执行
        env.execute("AvgPerWindow");
    }
}
