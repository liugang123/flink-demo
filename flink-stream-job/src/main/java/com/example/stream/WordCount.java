package com.example.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理实例
 * 从文件读取数据
 */
public class WordCount {

    public static void main(String[] args) throws Exception {

        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取数据
        String filePath = ".\\flink-stream-job\\src\\main\\resources";
        DataSet<String> dataSource = env.readTextFile(filePath);

        // 对单词分组，后sum操作
        DataSet<Tuple2<String, Integer>> dataResult = dataSource.flatMap(new MyFlatMapper())
                .groupBy(0)
                .sum(1);

        dataResult.print();
    }

    /**
     * 数据流转化操作
     */
    private static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        public void flatMap(String input, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = input.split(" ");
            for (String word : words) {
                collector.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}
