package com.example.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * 数据源为kafka
 */
public class TableApiKafkaSourceExample {

    public static void main(String[] args) throws Exception {

        // 1.执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3.kafka数据源
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("topic_flink_sensor")
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.18.75.231:9092")
                .property(ConsumerConfig.GROUP_ID_CONFIG, "group_flink_table_sensor")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                ).createTemporaryTable("inputTable");

        // 4.查询转换
        Table sensorTable = tableEnv.from("inputTable");
        Table resultTable = sensorTable.select("id,temp")
                .filter("id === 'xxx'");

        // 5.聚合统计
        Table avgTable = sensorTable.groupBy("id")
                .select("id,id.count as count, temp.avg as avgTemp");

        // 6.数据汇kafka
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("topic_flink_table_sink")
                .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.18.75.231:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE())
                ).createTemporaryTable("outputTable");

        // 7.输出流数据
        resultTable.insertInto("outputTable");

        // 8.执行作业
        tableEnv.execute("");
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
