package com.atugigu.day01;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

//todo 数据流里边元素的类型SensorReading
// 并行数据源的函数RichParallelSourceFunction
public class SensorSource extends RichParallelSourceFunction<SensorReading> {
    private boolean running = true;

    @Override
    public void run(SourceContext srcCtx) throws Exception {
        Random rand = new Random();

        String[] sensorIDs = new String[10];
        Double[] curFTemp = new Double[10];

        for (int i = 0; i < 10; i++) {
            sensorIDs[i]="sensor_"+(i+1);
            //todo 高斯随机数
            // 初始化温度
            curFTemp[i]=65+(rand.nextGaussian()*20);
        }

        //todo 产生数据源
        while (running){
            //todo 产生毫秒事件戳
            long curTime = Calendar.getInstance().getTimeInMillis();
            for (int i = 0; i < 10; i++) {
                curFTemp[i] += rand.nextGaussian()*0.5;
                //todo 向下游发送数据
                srcCtx.collect(new SensorReading(sensorIDs[i],curFTemp[i],curTime));

            }

            Thread.sleep(100);
        }



    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
