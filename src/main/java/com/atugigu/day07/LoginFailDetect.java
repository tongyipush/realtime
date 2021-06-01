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

public class LoginFailDetect {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置任务并行度
        env.setParallelism(1);

        //todo 引入数据源
        SingleOutputStreamOperator<LoginEvent> loginStream = env.fromElements(
                new LoginEvent("user_1", "0.0.0.0", "fail", 1000L),
                new LoginEvent("user_1", "0.0.0.1", "fail", 2000L),
                new LoginEvent("user_1", "0.0.0.2", "fail", 3000L),
                new LoginEvent("user_1", "0.0.0.3", "success", 4000L),
                new LoginEvent("user_1", "0.0.0.4", "fail", 5000L),
                new LoginEvent("user_1", "0.0.0.5", "fail", 6000L),
                new LoginEvent("user_1", "0.0.0.6", "fail", 7000L),
                new LoginEvent("user_1", "0.0.0.7", "fail", 8000L),
                new LoginEvent("user_2", "0.0.0.8", "success", 9000L)
        )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                return element.eventTime;
                            }
                        }));

        //todo 定义匹配的模板：5s内连续三次登陆失败
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("first")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .next("second")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .next("third")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                //todo 三个时间必须在5s之内完成
                .within(Time.seconds(5));

        //todo 两个参数： 待匹配的流，模板
        // 返回结果：匹配出的事组成的流
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginStream.keyBy(r -> r.userId), pattern);

        //todo 将符合模板的时间匹配出来
        patternStream
                .select(new PatternSelectFunction<LoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                        LoginEvent first = pattern.get("first").get(0);
                        LoginEvent second = pattern.get("second").get(0);
                        LoginEvent third = pattern.get("third").get(0);

                        return "用户："+first.userId+"分别在IP地址为："+first.ipAddress+"|"+second.ipAddress+"|"+third.ipAddress+"登录失败";
                    }
                })
                .print();


        //todo 执行
        env.execute("LoginEvent");

    }
}
