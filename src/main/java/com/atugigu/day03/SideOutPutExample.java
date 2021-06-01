package com.atugigu.day03;

import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.lang.model.element.VariableElement;

public class SideOutPutExample {

    private  static OutputTag<String> outputTag= new OutputTag<String>("side-output"){};

    public static void main(String[] args) throws Exception{

        //todo 创建连接环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置任务并行度
        env.setParallelism(1);

        //todo 引入数据源源
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        SingleOutputStreamOperator<SensorReading> result = stream
                .process(new ProcessFunction<SensorReading, SensorReading>() {
                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                        if (value.temperature < 65.0) {
                            //todo 发送字符串到测输出流
                            ctx.output(outputTag, "温度小于65度");
                        }

                        out.collect(value);
                    }
                });
        //todo 打印主流
//        result.print();
        //todo 打印侧输出流
        result.getSideOutput(outputTag).print();

        //todo 执行
        env.execute("sideOutPut");
    }
}
