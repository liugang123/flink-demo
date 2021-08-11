package com.example.table;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * tableApi简单实例
 */
public class TableApiExample {

    public static void main(String[] args) throws Exception {

        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 数据源
        DataStream<SensorRecord> dataStream = env.addSource(createKafkaConsumer()).map(message -> {
            String[] words = message.split(" ");
            SensorRecord sensorRecord = new SensorRecord();
            sensorRecord.setId(words[0]);
            sensorRecord.setTimestamp(Long.valueOf(words[1]));
            sensorRecord.setTemp(Double.valueOf(words[2]));
            return sensorRecord;
        });

        // 创建表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        // 基于流创建一张表
        Table dataTable = tableEnvironment.fromDataStream(dataStream);

        // 方式一：调用tableApi查询算子，获取结果表
        Table resultTable = dataTable.select("id,temp")
                .where("id = 'xxx'");

        // 方式二：执行sql，获取结果表
        tableEnvironment.createTemporaryView("sensor", dataTable);
        String sql = "select id,temp from sensor where id = 'xxx'";
        Table resultSqlTable = tableEnvironment.sqlQuery(sql);

        // 输出结果
        tableEnvironment.toAppendStream(resultTable, Row.class).print("result");
        tableEnvironment.toAppendStream(resultSqlTable, Row.class).print("sql");

        // 执行作业
        env.execute();
    }

    /**
     * kafka消费者
     */
    public static FlinkKafkaConsumer011<String> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.61.206:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_flink_table_sensor");
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<String>("topic_flink_sensor", new SimpleStringSchema(), properties);
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
