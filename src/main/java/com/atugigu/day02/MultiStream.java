package com.atugigu.day02;

import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

//todo connect使用方式：
// 1.将第一条流进行keyby,第二条流进行广播
// 2.将两条流全部keyby，相同key的放在一起处理
public class MultiStream {
    public static void main(String[] args) throws Exception {
        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置并行任务数量
        env.setParallelism(1);

        //todo 引入数据源
        // 温度传感器数据
        DataStreamSource<SensorReading> readings = env.addSource(new SensorSource());

        //todo 烟雾传感器数据
        // 将烟雾传感器的并行度设置为1，因为广播需要，需要任务在一个任务槽
        DataStreamSource<SmokeLevel> smoke = env.addSource(new SmokeLevelSource()).setParallelism(1);

        readings
                .connect(smoke.broadcast())
                .flatMap(new CoFlatMapFunction<SensorReading,SmokeLevel,Alert>() {

                    private SmokeLevel smokeLevel = SmokeLevel.LOW;

                    @Override
                    public void flatMap1(SensorReading value, Collector<Alert> out) throws Exception {
                        //todo 处理传感器的数据
                        if (this.smokeLevel == SmokeLevel.HIGH && value.temperature > 100){
                            out.collect(new Alert("起火了"+value,value.timestamp));
                        }
                    }

                    @Override
                    public void flatMap2(SmokeLevel value, Collector<Alert> out) throws Exception {
                        //todo 处理烟雾传感器的数据
                        this.smokeLevel = value;
                    }
                }).print();

        env.execute("MultiStream");

    }
}
