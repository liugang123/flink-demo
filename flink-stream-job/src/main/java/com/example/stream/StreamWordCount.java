package com.example.stream;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 流处理实例
 * socket流为数据源
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {

        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 启动参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // 数据源
        DataStream<String> dataStream = env.socketTextStream(host, port);
        // 分组、统计
        DataStream<Tuple2<String, Integer>> countDataStream = dataStream.flatMap(new MyFlatMapper())
                .keyBy(0)
                .sum(1);
        // 输出数据
        countDataStream.print();

        // 执行作业
        env.execute();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                collector.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}
