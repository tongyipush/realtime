package com.atugigu.day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class LoginFailDetect1 {
    public static void main(String[] args) throws Exception{

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        SingleOutputStreamOperator<LoginEvent> streamWithWaterMark = env.fromElements(
                new LoginEvent("user_1", "0.0.0.0", "fail", 1000L),
                new LoginEvent("user_1", "0.0.0.1", "fail", 2000L),
                new LoginEvent("user_1", "0.0.0.2", "success", 3000L),
                new LoginEvent("user_1", "0.0.0.3", "success", 4000L),
                new LoginEvent("user_1", "0.0.0.4", "fail", 5000L),
                new LoginEvent("user_1", "0.0.0.5", "fail", 6000L),
                new LoginEvent("user_1", "0.0.0.6", "fail", 7000L),
                new LoginEvent("user_2", "0.0.0.7", "success", 8000L)
        )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                return element.eventTime;
                            }
                        })
                );

        //todo 定义匹配的模板：5s之内连续三次登录失败
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("three")
                .times(3) //todo 连续三次，^LoginEvent\LoginEvent{2}
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .within(Time.seconds(5));
        
        //todo CEP 库实现匹配
        // 两个参数：待匹配的流，匹配模板
        // 返回结果，匹配出来的事组成的结果
        PatternStream<LoginEvent> patternStream = CEP.pattern(streamWithWaterMark.keyBy(r -> r.userId), pattern);

        //todo 选择匹配出来的结果
        patternStream
                .select(new PatternSelectFunction<LoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<LoginEvent>> pattern) throws Exception {

                        LoginEvent first = pattern.get("three").get(0);
                        LoginEvent second = pattern.get("three").get(1);
                        LoginEvent third = pattern.get("three").get(2);
                        return "用户ID为:"+first.userId+"分别在IP地址为："+first.ipAddress+"|"+second.ipAddress+"|"+third.ipAddress+"登录失败!";
                    }
                })
                .print();


        //todo 执行
        env.execute("LoginFailDetect1");
    }
}
