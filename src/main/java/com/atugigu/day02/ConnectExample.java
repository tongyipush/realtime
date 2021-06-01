package com.atugigu.day02;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectExample {
    public static void main(String[] args) throws Exception {
        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置并行任务数量
        env.setParallelism(1);

        //todo 引入数据源
         DataStreamSource<Tuple2<String, Integer>> stream1 = env.fromElements(Tuple2.of("a", 1), Tuple2.of("b", 2));
         DataStreamSource<Tuple2<String, String>> stream2 = env.fromElements(Tuple2.of("a", "a"), Tuple2.of("b", "b"));
         
         stream1
                 .keyBy(r -> r.f0)
                 .connect(stream2.keyBy(r -> r.f0))
                 .map(new CoMapFunction<Tuple2<String, Integer>, Tuple2<String, String>, String>() {
                     @Override
                     public String map1(Tuple2<String, Integer> value) throws Exception {
                         return "方法一";
                     }

                     @Override
                     public String map2(Tuple2<String, String> value) throws Exception {
                         return "方法二";
                     }
                 })
                 .print();

         env.execute("connect example");
    }
}
