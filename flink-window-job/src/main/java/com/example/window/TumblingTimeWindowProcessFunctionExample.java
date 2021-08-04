package com.example.window;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 滚动时间窗口(自动窗口)--全量窗口函数
 */
public class TumblingTimeWindowProcessFunctionExample {

    public static void main(String[] args) throws Exception {

        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 数据源
        DataStream<SensorRecord> dataStream = env.addSource(createKafkaConsumer())
                .map(message -> {
                    SensorRecord sensorRecord = new SensorRecord();
                    String[] words = message.split(",");
                    sensorRecord.setId(words[0]);
                    sensorRecord.setTimestamp(Long.valueOf(words[1]));
                    sensorRecord.setTemp(Double.valueOf(words[2]));
                    return sensorRecord;
                });

        // 开窗函数
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> dataStreamResult = dataStream.keyBy(k -> k.getId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .apply(new WindowFunction<SensorRecord, Tuple3<String, Long, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow timeWindow, Iterable<SensorRecord> iterable, Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
                        long windowEnd = timeWindow.getEnd();
                        AtomicInteger count = new AtomicInteger();
                        iterable.forEach(item -> count.getAndIncrement());
                        collector.collect(new Tuple3<>(key, windowEnd, count.intValue()));
                    }
                });

        // 输出数据
        dataStreamResult.print("windows process result");

        // 执行作业
        env.execute();
    }

    private static FlinkKafkaConsumer011<String> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.61.206:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_flink_window_sensor");
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>("topic_flink_sensor", new SimpleStringSchema(), properties);
        return consumer;
    }

    private static class SensorRecord {

        private String id;
        private Long timestamp;
        private Double temp;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }

        public Double getTemp() {
            return temp;
        }

        public void setTemp(Double temp) {
            this.temp = temp;
        }
    }

}
