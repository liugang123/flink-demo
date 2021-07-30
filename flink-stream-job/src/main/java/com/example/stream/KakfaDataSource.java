package com.example.stream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * kafka数据源实例
 */
public class KakfaDataSource {

    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 数据源
        DataStream<String> dataStream = env.addSource(createKafkaConsumer());

        // 打印数据
        // dataStream.print();

        // kafka数据汇
        dataStream.map(message -> {
            System.out.println("message:" + message);
            return message;
        }).addSink(createKafkaProducer());

        // 执行作业
        env.execute();
    }

    /**
     * 创建kafka消费者
     *
     * @return
     */
    private static FlinkKafkaConsumer011<String> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.61.206:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_flink_sensor");
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>("topic_flink_sensor", new SimpleStringSchema(), properties);
        // 进行检查点时提交kafka偏移量
        // consumer.setCommitOffsetsOnCheckpoints(true);
        consumer.setStartFromGroupOffsets();
        return consumer;
    }

    /**
     * 创建kafka生产者
     *
     * @return
     */
    private static FlinkKafkaProducer011<String> createKafkaProducer() {
        FlinkKafkaProducer011<String> producer = new FlinkKafkaProducer011<String>("172.17.61.206:9092", "topic_flink_sink_sensor", new SimpleStringSchema());
        return producer;
    }
}
