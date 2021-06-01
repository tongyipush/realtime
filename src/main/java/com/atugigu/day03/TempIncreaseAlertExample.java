package com.atugigu.day03;


import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

//todo 状态变量在程序宕机的时候或者在做检查点操作的时候，这些状态变量都会保存到状态后端
// 比如把状态变量保存到hdfs上边
// 等到下次程序宕机重启的时候，就可以去hdfs上边寻找这个变量，getState()就是一个寻找的过程
// 如果存在就读取这个变量，不存在就初始化这个变量
// 状态变量是一个单例模式
// 状态变量的名字映射到状态变量的后端的存储里边
// 单例模式：没有的话就初始化一个新的，有的就取出使用
// 实际就是为了宕机时候恢复使用
// 初始化一个普通的变量，那么一宕机的话数据就没了，因为Java变量保存在内存中
public class TempIncreaseAlertExample {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        stream
                .keyBy(r -> r.id)
                .process(new MyAlert())
                .print();

        //todo 执行
        env.execute("TempIncreaseAlert");
    }

    public static class MyAlert extends KeyedProcessFunction<String,SensorReading,String> {
        //todo 定义状态变量
        // 最近一次温度
        private ValueState<Double> curTemp;
        //todo 定时器定时时间
        private ValueState<Long> curTimer;

        //todo 初始化，获取当前状态变量的值，如果没有的话就初始话一个值


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            curTemp = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("state-temp", Types.DOUBLE)
            );

            curTimer = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("state-time",Types.LONG)
            );
        }

        @Override
        public void processElement(SensorReading reading, Context ctx, Collector<String> out) throws Exception {
            Double preTemp = 0.0;
            //todo 不是第一个温度值，更新温度值
            if ( curTemp.value() != null){

                preTemp = curTemp.value();
            }

            curTemp.update(reading.temperature);

            //todo 获取定时器时间
            Long curTimerStamp = 0L;
            if ( curTimer.value() != null){
                //todo 说明已经注册过报警定时器了，获取报警定时器时间
                curTimerStamp = curTimer.value();
            }

            //todo 删除定时器，如果温度下降或者来的是第一条温度
            if ( preTemp ==0.0  || reading.temperature < preTemp ){

                ctx.timerService().deleteProcessingTimeTimer(curTimerStamp);
                //todo 清空定时器时间
                curTimer.clear();

                //todo 如果温度上升，并且不存在报警时间，新建定时器
            } else  if ( reading.temperature > preTemp && curTimerStamp == 0L){

                long oneSecondAfter = ctx.timerService().currentProcessingTime() + 1000L;
                ctx.timerService().registerProcessingTimeTimer(oneSecondAfter);

                //todo 保存注册的定时器的时间
                curTimer.update(oneSecondAfter);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);

            out.collect("ID为:"+ctx.getCurrentKey()+"连续一秒温度上升了！");

            curTimer.clear();
        }
    }
}
