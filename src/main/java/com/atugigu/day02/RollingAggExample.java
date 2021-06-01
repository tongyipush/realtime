package com.atugigu.day02;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RollingAggExample {
    public static void main(String[] args) throws Exception {
        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置并行任务数量
        env.setParallelism(1);

        //todo 引入数据源
        DataStreamSource<Tuple3<Integer, Integer, Integer>> inputStream = env.fromElements(
                Tuple3.of(1, 2, 2),
                Tuple3.of(2, 3, 1),
                Tuple3.of(2, 2, 4),
                Tuple3.of(1, 5, 3)
        );
        //todo 给每个key维护一个累加器
        //todo 聚合
        inputStream
                .keyBy(r -> r.f0)
                .sum(1)
                .print();

        inputStream
                .keyBy(r -> r.f0)
                .reduce(new ReduceFunction<Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public Tuple3<Integer, Integer, Integer> reduce(Tuple3<Integer, Integer, Integer> value1, Tuple3<Integer, Integer, Integer> value2) throws Exception {
                        return  Tuple3.of(value1.f0,value1.f1+value2.f1,value1.f2);
                    }
                })
                .print();

        env.execute("rolling aggregate example");

    }
}
