package io.github.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.sink.PulsarSink;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class RabbitmqPulsarJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("localhost")
                .setPort(5000)
                .build();
        final DataStream<String> stream = env.addSource(new RMQSource<>(
                        connectionConfig,
                        "queueName",
                        true,
                        new SimpleStringSchema())).setParallelism(1);
        PulsarSink<String> sink = PulsarSink.builder()
                .setServiceUrl("pulsar://localhost:6650")
                .setAdminUrl("http://localhost:8080")
                .setTopics("sink-topic")
                .setSerializationSchema(PulsarSerializationSchema.flinkSchema(new SimpleStringSchema()))
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();
        stream.sinkTo(sink);
        env.execute("rabbitmq pulsar job start");
    }

}
