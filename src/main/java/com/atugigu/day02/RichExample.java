package com.atugigu.day02;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//todo 富函数
public class RichExample {
    public static void main(String[] args) throws Exception {
        //todo 获取运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置运行时并行任务数量
        env.setParallelism(1);

        //todo 引入数据源
        DataStreamSource<Integer> readings = env.fromElements(1, 2, 3, 4);

        readings
                .map(new RichMapFunction<Integer, Integer>() {

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("进入生命周期");
                    }

                    @Override
                    public Integer map(Integer value) throws Exception {
                        return value+1;
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println("结束生命周期");
                    }
                })

                .print();

        //todo 执行环境
        env.execute("rich example");
    }
}
