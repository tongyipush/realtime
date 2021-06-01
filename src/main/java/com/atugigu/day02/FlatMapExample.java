package com.atugigu.day02;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapExample {
    public static void main(String[] args) throws Exception{

        //todo 获取连接环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        DataStreamSource<String> readings = env.fromElements("white", "black", "gray");

        readings
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        if (value.equals("white")){
                            out.collect("white");
                        } else if (value.equals("black")){
                            out.collect("black");
                            out.collect("black");
                        }
                    }
                })
                .print();

        readings
                .flatMap(
                        (FlatMapFunction<String, String>) (value,out) ->{
                            if (value.equals("white")){
                                out.collect(value);
                            } else if (value.equals("black")){
                                out.collect(value);
                                out.collect(value);
                            }
                        }
                )
                .returns(Types.STRING)
                .print();




        //todo 执行
        env.execute("FlatMap");

    }
}
