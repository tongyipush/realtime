package com.atugigu.day09;

import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class AvgTemp {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置任务并行数量
        env.setParallelism(1);
        //todo table环境配置
        // 创建对象
        // 使用new的时候，这个要new的类可以没有加载
        // 使用new Instance时候，就必须保证：1、这个类已经加载；2、这个类已经连接了
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        //todo 获取表运行时环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        //todo 引入数据源
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        //todo Table API
        // 将DataStream转换成Table
        Table table = tEnv.fromDataStream(stream, $("id"), $("temperature").as("temp"), $("timestamp").as("ts"));

        //todo 查询
        Table tableResult = table
                .groupBy($("id"))
                .select($("id"), $("temp").avg());

        //todo 转换成DataStream
        // 只要查询中有聚合操作，必须使用toRetreactStream()
        tEnv.toRetractStream(tableResult, Row.class).print();

        //todo SQL
        // 将DataStream转换成临时视图
        tEnv.createTemporaryView("sensor",stream, $("id"), $("temperature").as("temp"), $("timestamp").as("ts"));


        //todo 查询
        Table sqlResult = tEnv.sqlQuery("SELECT id,avg(temp) FROM sensor GROUP BY id ");
        //todo 将查询结果转换成result；
        tEnv.toRetractStream(sqlResult,Row.class).print();

        //todo 执行
        env.execute("AvgTemp");


    }
}
