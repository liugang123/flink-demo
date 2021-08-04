package com.example.window;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Date;
import java.util.Properties;

/**
 * 事件时间窗口&水位线设置
 * 统计最低温度
 */
public class EventTimeWindowWatermakExample {

    public static void main(String[] args) throws Exception {

        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 数据源
        DataStream<SensorRecord> dataStream = env.addSource(createKafkaConsumer()).map(message -> {
            String[] words = message.split(" ");
            SensorRecord sensorRecord = new SensorRecord();
            sensorRecord.setId(words[0]);
            sensorRecord.setTimestamp(Long.valueOf(words[1]));
            sensorRecord.setTemp(Double.valueOf(words[2]));
            return sensorRecord;
        });

        // 设置时间戳水位线
        dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorRecord>(Time.seconds(2)) {
            /**
             * 数据流对象的事件戳设置
             * @param sensorRecord 数据流对象
             * @return
             */
            @Override
            public long extractTimestamp(SensorRecord sensorRecord) {
                // 当前时间，时间戳
                return new Date().getTime();
            }
        });

        // 侧输出流标签
        OutputTag<SensorRecord> outputTag = new OutputTag<SensorRecord>("late") {
        };

        // 开窗函数
        SingleOutputStreamOperator<SensorRecord> dataStreamResult = dataStream.keyBy(k -> k.getId())
                // 时间滚动窗口
                .timeWindow(Time.seconds(15))
                // 允许迟到数据1分钟
                .allowedLateness(Time.minutes(1))
                // 超过迟到时间的数据，汇总到侧输出流
                .sideOutputLateData(outputTag)
                // 计算温度最小值
                .minBy("temp");

        dataStreamResult.print("min temp");

        // 侧输出流数据
        dataStreamResult.getSideOutput(outputTag).print("late");

        // 执行作业
        env.execute();
    }

    /**
     * 创建kafka消息者
     *
     * @return
     */
    private static FlinkKafkaConsumer011<String> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.61.206:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_flink_window_sensor");
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>("topic_flink_sensor", new SimpleStringSchema(), properties);
        return consumer;
    }

    public static class SensorRecord {

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
