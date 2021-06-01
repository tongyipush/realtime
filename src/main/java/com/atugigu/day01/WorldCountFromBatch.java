package com.atugigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WorldCountFromBatch {
    public static void main(String[] args) throws Exception {
        // 获取运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行任务的数量
        env.setParallelism(1);

        // 引入数据源
        DataStreamSource<String> text = env
                .fromElements(
                        "Hello World",
                        "Hello World",
                        "Hello World"
                );

        // 第一步：Map操作
        SingleOutputStreamOperator<WordWithCount> mapped = text
                // 第一个泛型：输入类型
                // 第二个泛型：输出类型
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                        for (String word : value.split(" ")) {
                            // 使用集合来向下游输出元素
                            out.collect(new WordWithCount(word, 1L));
                        }
                    }
                });

        // 第二步：Shuffle操作
        KeyedStream<WordWithCount, String> keyed = mapped.keyBy(r -> r.word);

        // 第三步：Reduce操作
        // reduce操作会维护一个累加器
        SingleOutputStreamOperator<WordWithCount> reduced = keyed
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount value1, WordWithCount value2) throws Exception {
                        return new WordWithCount(value1.word, value1.count + value2.count);
                    }
                });

        // 输出
        reduced.print();

        // 别忘了执行程序
        env.execute("单词计数");
    }

    public static class WordWithCount {
        public String word;
        public Long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, Long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
