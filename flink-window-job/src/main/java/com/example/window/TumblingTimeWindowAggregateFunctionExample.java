package com.example.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * 滚动时间窗口(自动窗口)--增量聚合函数
 */
public class TumblingTimeWindowAggregateFunctionExample {

    public static void main(String[] args) throws Exception {

        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // kafka数据源
        DataStream<String> dataStream = env.addSource(createKafkaConsumer());

        // 转化数据流
        DataStream<SensorRecord> sensorRecordDataStream = dataStream.map(message -> {
            SensorRecord sensorRecord = new SensorRecord();
            String[] words = message.split(",");
            sensorRecord.setId(words[0]);
            sensorRecord.setTimestamp(Long.valueOf(words[1]));
            sensorRecord.setTemp(Double.valueOf(words[2]));
            return sensorRecord;
        });

        // 开窗操作
        DataStream<Double> dataStreamResult = sensorRecordDataStream.keyBy(k -> k.getId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                // 增量聚合函数
                .aggregate(new AggregateFunction<SensorRecord, Double, Double>() {

                    // 累加器
                    @Override
                    public Double createAccumulator() {
                        return 0d;
                    }

                    // 基于上次数据+1
                    @Override
                    public Double add(SensorRecord sensorRecord, Double accumulator) {
                        accumulator = accumulator > sensorRecord.getTemp() ? accumulator : sensorRecord.getTemp();
                        return accumulator;
                    }

                    // 结果数据
                    @Override
                    public Double getResult(Double accumulator) {
                        return accumulator;
                    }

                    // 分区合并结果
                    @Override
                    public Double merge(Double integer, Double acc1) {
                        return null;
                    }
                });

        // 结果输出
        dataStreamResult.print("window result");

        // 执行作业
        env.execute();
    }

    /**
     * 创建kafka数据源
     *
     * @return
     */
    private static FlinkKafkaConsumer011<String> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.61.206:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_flink_window_sensor");

        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<String>("topic_flink_sensor", new SimpleStringSchema(), properties);
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
