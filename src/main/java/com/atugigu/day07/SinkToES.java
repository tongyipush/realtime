package com.atugigu.day07;

import com.atugigu.day01.SensorReading;
import com.atugigu.day01.SensorSource;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

public class SinkToES {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置任务并行数量
        env.setParallelism(1);

        //todo 引入数据源
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        //todo 创建list
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102",9200,"http"));
        ElasticsearchSink.Builder<SensorReading> esBuilder = new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<SensorReading>() {
            @Override
            public void process(SensorReading element, RuntimeContext ctx, RequestIndexer indexer) {
                HashMap<String, String> data = new HashMap<>();
                //todo 将数据放入HashMap中
                data.put(element.id, element.temperature.toString());

                //todo org.elasticsearch.client
                IndexRequest indexRequest = Requests.indexRequest()
                        .index("sensor-reading")//todo 如果不存在这个索引，es会自动创建
                        .type("sensor")
                        .source(data);

                indexer.add(indexRequest);
            }
        });

        //todo 写入es的每一批数据只有一条
        // 通过调节这个参数来优化写入es的性能
        esBuilder.setBulkFlushMaxActions(1);

        stream.addSink(esBuilder.build());

        //todo 执行
        env.execute("SinkToES");
    }
}
