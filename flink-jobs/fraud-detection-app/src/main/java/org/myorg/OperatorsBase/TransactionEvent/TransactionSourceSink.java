package org.myorg.OperatorsBase.TransactionEvent;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.myorg.EnrichmentMethods.RedisExampleMapper;
import org.myorg.Events.TransactionEvent.TransactionEventRaw;
import org.myorg.Events.TransactionEvent.TransactionResult;

public class TransactionSourceSink {

    public static void runRedisSink(DataStream<TransactionResult> input, String host, int port, String password) {
        DataStream<Tuple2<String, String>> redisSink = RedisExampleMapper.getSinkStream(input);
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost(host)
                .setPort(port)
                .setPassword(password)
                .build();
        redisSink.addSink(new RedisSink<>(conf, new RedisExampleMapper()));
    }

    public static KafkaSource<TransactionEventRaw> getDbTransactionEventsSource(String host, int port, String topic) {
        return KafkaSource.<TransactionEventRaw>builder()
                .setBootstrapServers(String.format("%s:%s", host, port))
                .setDeserializer(new TransactionEventDeserializationSchema())
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();
    }
    public static KafkaSource<TransactionEventRaw> getTransactionEventsSource(String host, int port, String topic) {
        return KafkaSource.<TransactionEventRaw>builder()
                .setBootstrapServers(String.format("%s:%s", host, port))
                .setDeserializer(new TransactionEventDeserializationSchema())
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();
    }

    public static KafkaSink<TransactionResult> getKafkaSink(String host, int port, String outTopic) {
        return KafkaSink.<TransactionResult>builder()
                .setBootstrapServers(String.format("%s:%s", host, port))
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(outTopic)
                                .setKafkaValueSerializer(TransactionEventSerializationSchema.class)
                                .build()
                )
                .build();
    }
}
