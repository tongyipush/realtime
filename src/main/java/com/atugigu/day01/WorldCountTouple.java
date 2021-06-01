package com.atugigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WorldCountTouple {
    public static void main(String[] args) throws Exception {
        //todo 获取运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置并行任务数量
        env.setParallelism(1);

        //todo 引入数据源
        DataStreamSource<String> text = env.fromElements("Hello World",
                "Hello World",
                "Hello World"
        );

        text
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {

                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        for (String word : value.split("//s")) {
                            out.collect(Tuple2.of(word,1L));
                        }
                                
                    }
                })
                .keyBy(r -> r.f0)
                .sum(1)
                .print();

        env.execute("Word Count");
    }
}
