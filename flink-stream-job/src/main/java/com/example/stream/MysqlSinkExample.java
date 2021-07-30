package com.example.stream;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * mysql数据汇实例
 */
public class MysqlSinkExample {

    public static void main(String[] args) throws Exception {

        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // kafka数据源
        DataStream<Tuple2<Long, Double>> dataStream = env.addSource(createKafkaConsumer())
                .map(message -> {
                    System.out.println("message:" + message);
                    Tuple2<Long, Double> tuple2 = new Tuple2<>();
                    String[] words = message.split(" ");
                    tuple2.setFields(Long.valueOf(words[0]), Double.valueOf(words[1]));
                    return tuple2;
                }).returns(Types.TUPLE(Types.LONG, Types.DOUBLE));

        // mysql数据汇
        dataStream.addSink(new MysqlSink());

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
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_flink_sensor");
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>("topic_flink_sensor", new SimpleStringSchema(), properties);
        return consumer;
    }

    /**
     * mysql数据汇
     */
    private static class MysqlSink extends RichSinkFunction<Tuple2<Long, Double>> {
        // 连接对象
        Connection connection = null;
        // 预编译语句
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            String connStr = "jdbc:mysql://172.17.61.206:3306/flink_test?useSSL=false";
            connection = DriverManager.getConnection(connStr, "root", "root");
            insertStmt = connection.prepareStatement("insert into sensor_temp(id,temp) values (?, ?)");
            updateStmt = connection.prepareStatement("update sensor_temp set temp = ? where id = ?");
        }

        @Override
        public void invoke(Tuple2<Long, Double> tuple, Context context) throws Exception {
            updateStmt.setDouble(1, tuple.getField(1));
            updateStmt.setLong(2, tuple.getField(0));
            updateStmt.execute();
            if (updateStmt.getUpdateCount() <= 0) {
                insertStmt.setLong(1, tuple.getField(0));
                insertStmt.setDouble(2, tuple.getField(1));
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }
    }
}
