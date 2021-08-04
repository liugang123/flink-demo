package com.example.window;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;


import java.util.Properties;

/**
 * 滑动计数窗口(手动窗口)--增量聚合函数
 */
public class SlidingCountWindowAggregateFunctionExample {

    public static void main(String[] args) throws Exception {

        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 数据源
        DataStream<SensorRecord> dataStream = env.addSource(createKafkaConsumer()).map(message -> {
            String[] words = message.split(",");
            SensorRecord sensorRecord = new SensorRecord();
            sensorRecord.setId(words[0]);
            sensorRecord.setTimestamp(Long.valueOf(words[1]));
            sensorRecord.setTemp(Double.valueOf(words[2]));
            return sensorRecord;
        });

        // 滑动窗口，当窗口不足设置的大小时，会先按照步长输出
        // 开窗函数，窗口大小是10个元素，每2个元素滑动窗口
        // 窗口大小10，步长2，那么前5次输出时，窗口内的元素个数分别是（2，4，6，8，10），再往后就是10个为一个窗口了
        DataStream<Double> dataStreamResult = dataStream.keyBy(k -> k.getId())
                .countWindow(10, 2)
                .aggregate(new AvgFunction());

        // 输出结果
        dataStreamResult.print("sliding count window");

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
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_flink_window_sensor");
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>("topic_flink_sensor", new SimpleStringSchema(), properties);
        return consumer;
    }

    /**
     * 增量聚合函数
     */
    private static class AvgFunction implements AggregateFunction<SensorRecord, Tuple2<Double, Integer>, Double> {

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2(0.0, 0);
        }

        @Override
        public Tuple2<Double, Integer> add(SensorRecord sensorRecord, Tuple2<Double, Integer> accumulator) {
            // 温度值求和，温度个数+1
            return new Tuple2<>(accumulator.f0 + sensorRecord.getTemp(), accumulator.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> accumulator) {
            // 温度平均值
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
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
