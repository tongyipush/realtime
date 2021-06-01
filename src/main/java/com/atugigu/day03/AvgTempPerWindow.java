package com.atugigu.day03;

import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class AvgTempPerWindow {
    public static void main(String[] args) throws Exception{

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        SingleOutputStreamOperator<SensorReading> stream = env.addSource(new SensorSource()).filter(r -> r.id.equals("sensor_1"));

        //todo 分流开窗聚合
        stream
                //todo 将相同的key的数据放在一个任务槽里边
                .keyBy(r -> r.id)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new Win())
                .print();

        //todo 执行
        env.execute("AvgTempPerWindow");
    }

    public static class Win extends ProcessWindowFunction<SensorReading,String,String,TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<SensorReading> elements, Collector<String> out) throws Exception {
            double sum = 0.0;
            long count = 0L;
            for (SensorReading k : elements) {
                sum += k.temperature;
                count += 1L;
            }

            String start = new Timestamp(context.window().getStart()).toString();
            String end = new Timestamp(context.window().getEnd()).toString();

            out.collect("窗口时间:"+start+"------"+end+"传感器id:"+key+"平均值:"+sum/count);

        }
    }
}
