package io.github.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.pulsar.client.api.SubscriptionType;

public class PulsarRabbitmqJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        PulsarSource<String> source = PulsarSource.builder()
                .setServiceUrl("pulsar://localhost:6650")
                .setAdminUrl("http://localhost:8080")
                .setStartCursor(StartCursor.earliest())
                .setTopics("source-topic")
                .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
                .setSubscriptionName("source-subscription")
                .setSubscriptionType(SubscriptionType.Exclusive)
                .build();

        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("localhost")
                .setPort(5000)
                .build();
        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Pulsar Source");
        stream.addSink(new RMQSink<>(connectionConfig, "sink-queue", new SimpleStringSchema()));
        env.execute("pulsar rabbitmq job start");
    }

}
