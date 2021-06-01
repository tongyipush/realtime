package com.atugigu.day06;

import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class StateBackendExample {
    public static void main(String[] args) throws Exception{

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置任务并行数量
        env.setParallelism(1);

        //todo 检查点保存时间间隔
        env.enableCheckpointing(10000L);

        //todo 设置检查点FsStateBackend
        // 将checkpoint远程存储到持久化文件系统hdfs(FileSystem),而对于本地状态，跟memoryStateBackend一样，
        // 也会存储到JobManager的JVM堆上
        // 同时拥有内存级的访问速度，和更好的容错保证
        env.setStateBackend(new FsStateBackend("file:///D:\\atguigu\\IdeaProjects\\flink-0921\\src\\main\\resources\\FsStateBackend",false));
        //todo 引入数据源
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());
        stream.print();

        //todo 执行
        env.execute();
    }
}
