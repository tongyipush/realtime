package com.atugigu.day07;

import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class WriteToMysqlExample {
    public static void main(String[] args) throws Exception {

        //todo 获取运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        //todo 向数据库输入数据
        stream.addSink(new MyJDBCSink());

        //todo 执行
        env.execute("SinkMySql");
    }

    public static class MyJDBCSink extends RichSinkFunction<SensorReading> {
        private Connection conn;
        private PreparedStatement insertStmt;
        private PreparedStatement updateStmt;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/sensor","root","123456");

            insertStmt = conn.prepareStatement("INSERT INTO temps (id,temp) VALUES(?,?)");
            updateStmt = conn.prepareStatement("UPDATE temps SET temp = ? WHERE id = ?");
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            updateStmt.setDouble(1,value.temperature);
            updateStmt.setString(2,value.id);
            updateStmt.execute();

            //todo 判断是否首次写入
            if (updateStmt.getUpdateCount() == 0){
                insertStmt.setString(1,value.id);
                insertStmt.setDouble(2,value.temperature);
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            insertStmt.close();
            updateStmt.close();
            conn.close();
        }
    }
}
