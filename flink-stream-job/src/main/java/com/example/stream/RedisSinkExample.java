package com.example.stream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * redis数据汇实例
 */
public class RedisSinkExample {

    public static void main(String[] args) throws Exception {

        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // kafka数据源
        DataStream<Tuple2<String, String>> dataStream = env.addSource(createKafkaConsumer())
                .map(message -> {
                    System.out.println(message);
                    String[] words = message.split(" ");
                    Tuple2<String, String> tuple = new Tuple2<>();
                    tuple.setFields(words[0], words[1]);
                    return tuple;
                }).returns(Types.TUPLE(Types.STRING, Types.STRING));
        // redis数据汇
        dataStream.addSink(createRedisSink());

        // 执行作业
        env.execute();
    }

    /**
     * kafka消费者
     *
     * @return
     */
    private static FlinkKafkaConsumer011<String> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.61.206:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_flink_sensor");
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>("topic_flink_sensor", new SimpleStringSchema(), properties);
        return consumer;
    }

    /**
     * redis数据汇
     *
     * @return
     */
    private static RedisSink<Tuple2<String, String>> createRedisSink() {
        // jedis参数
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("127.0.0.1")
                .setPort(6379)
                //.setPassword("")
                .setDatabase(0)
                .build();
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(config, new MyRedisMapper());
        return redisSink;
    }

    /**
     * 自定义ReidsMapper
     */
    public static class MyRedisMapper implements RedisMapper<Tuple2<String, String>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "flink");
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> tuple2) {
            return tuple2.getField(0);
        }

        @Override
        public String getValueFromData(Tuple2<String, String> tuple2) {
            return tuple2.getField(1);
        }

    }
}
