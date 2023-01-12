package org.myorg.EnrichmentMethods;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.Events.TransactionEvent.EventBase;
import org.myorg.Events.TransactionEvent.SuspiciousType;
import org.myorg.Events.TransactionEvent.TransactionEventRaw;
import org.myorg.OperatorsBase.ExternalConfiguration;
import org.myorg.OperatorsBase.TransactionEvent.TransactionSourceSink;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class EnrichmentFactory {

    public static DataStream<TransactionEventRaw> enrich(
            StreamExecutionEnvironment env,
            DataStream<TransactionEventRaw> input,
            String type,
            ExternalConfiguration externalConfiguration,
            boolean logLatencies
    ) {
        switch (type) {
            case "sync":
                return runSyncEnrichment(type.toUpperCase(), input, externalConfiguration, logLatencies);
            case "async":
                return runAsyncEnrichment(type.toUpperCase(), input, externalConfiguration, logLatencies);
            case "async-cache":
                return runAsyncCacheEnrichment(type.toUpperCase(), input, externalConfiguration, logLatencies);
            case "async-cache-partition":
                return runAsyncCachePartitionEnrichment(type.toUpperCase(), input, externalConfiguration, logLatencies);
            case "async-cache-redis":
                return runAsyncRedisEnrichment(type.toUpperCase(), input, externalConfiguration, logLatencies);
            case "stream":
                return runStreamEnrichment(type.toUpperCase(), env, input, externalConfiguration, logLatencies);

            default: return null;
        }
    }

    private static DataStream<TransactionEventRaw> runStreamEnrichment(
            String name,
            StreamExecutionEnvironment env,
            DataStream<TransactionEventRaw> input,
            ExternalConfiguration config,
            boolean logLatencies
            ) {
        KafkaSource<TransactionEventRaw> dbSource = TransactionSourceSink.getDbTransactionEventsSource(
                config.getKafkaHost(), config.getKafkaPort(), config.getKafkaDbTopic()
        );
        DataStream<TransactionEventRaw> dbInput = env.fromSource(
                dbSource,
                WatermarkStrategy
                        .<TransactionEventRaw>forBoundedOutOfOrderness(Duration.ofMillis(1000))
                        .withTimestampAssigner(((element, recordTimestamp) -> element.getTransaction().getTimestamp()))
                        .withIdleness(Duration.ofSeconds(1)),
                "Kafka-DB-Source"
        );

        DataStream<EventBase> transactionsDb = TransactionSplitter.getTransactionStream(dbInput);
        DataStream<EventBase> devicesDb = TransactionSplitter.getDeviceStream(dbInput);
        DataStream<EventBase> locationsDb = TransactionSplitter.getLocationStream(dbInput);

        DataStream<TransactionEventRaw> result = input
                .keyBy(x -> x.getTransaction().getCacheKey())
                .connect(transactionsDb.keyBy(EventBase::getCacheKey))
                .process(new StreamEnrichmentProcessFunction(SuspiciousType.UNKNOWN_RECIPIENT));

        result = result
                .keyBy(x -> x.getDevice().getCacheKey())
                .connect(devicesDb.keyBy(EventBase::getCacheKey))
                .process(new StreamEnrichmentProcessFunction(SuspiciousType.UNKNOWN_DEVICE));

        result = result
                .keyBy(x -> x.getLocation().getCacheKey())
                .connect(locationsDb.keyBy(EventBase::getCacheKey))
                .process(new StreamEnrichmentProcessFunction(SuspiciousType.UNKNOWN_LOCATION));

        return result;
    }

    private static DataStream<TransactionEventRaw> runAsyncEnrichment(
            String name,
            DataStream<TransactionEventRaw> input,
            ExternalConfiguration config,
            boolean logLatencies
            ) {

        return AsyncDataStream.unorderedWait(
                input,
                new CassandraEnrichmentAsync(
                        name,
                        logLatencies,
                        config.getDbHost(),
                        config.getDbPort(),
                        config.getDbUser(),
                        config.getDbPassword(),
                        config.getDbKeyspace()
                ),
                config.getAsyncTimeOutSec(),
                TimeUnit.SECONDS,
                config.getAsyncCapacity()
        ).name("Async-Enrichment");
    }

    private static DataStream<TransactionEventRaw> runAsyncRedisEnrichment(
            String name,
            DataStream<TransactionEventRaw> input,
            ExternalConfiguration config,
            boolean logLatencies
            ) {
        DataStream<TransactionEventRaw> addCachedEntries = AsyncDataStream.unorderedWait(
                input,
                new RedisCacheEnrichmentAsync(
                        config.getRedisHost(),
                        config.getRedisPort(),
                        config.getRedisPassword(),
                        config.getRedisHashKey()
                ),
                config.getAsyncTimeOutSec(),
                TimeUnit.SECONDS,
                10
        ).name("Add-Async-Cache-Redis-Entries-Enrichment");

        return AsyncDataStream.unorderedWait(
                addCachedEntries,
                new CassandraEnrichmentAsyncCacheRedis(
                        name,
                        logLatencies,
                        config.getDbHost(),
                        config.getDbPort(),
                        config.getDbUser(),
                        config.getDbPassword(),
                        config.getDbKeyspace(),
                        config.getRedisHost(),
                        config.getRedisPort(),
                        config.getRedisPassword(),
                        config.getRedisHashKey()
                ),
                config.getAsyncTimeOutSec(),
                TimeUnit.SECONDS,
                config.getAsyncCapacity()
        ).name("Async-Cassandra-Enrichment-Enrichment");
    }

    private static DataStream<TransactionEventRaw> runAsyncCachePartitionEnrichment(
            String name,
            DataStream<TransactionEventRaw> input,
            ExternalConfiguration config,
            boolean logLatencies
            ) {
        input = input.partitionCustom(new Partitioner<String>() {
            /**
             * Computes the partition for the given key.
             *
             * @param key           The key.
             * @param numPartitions The number of partitions to partition into.
             * @return The partition index.
             */
            @Override
            public int partition(String key, int numPartitions) {
                return Integer.parseInt(key) % numPartitions;
            }
        }, TransactionEventRaw::getKey);

        return AsyncDataStream.unorderedWait(
                input,
                new CassandraEnrichmentAsyncCache(
                        name,
                        logLatencies,
                        config.getDbHost(),
                        config.getDbPort(),
                        config.getDbUser(),
                        config.getDbPassword(),
                        config.getDbKeyspace(),
                        config.getCacheMaxSize()
                ),
                config.getAsyncTimeOutSec(),
                TimeUnit.SECONDS,
                config.getAsyncCapacity()
        ).name("Async-Cache-Partition-Enrichment");
    }

    private static DataStream<TransactionEventRaw> runAsyncCacheEnrichment(
            String name,
            DataStream<TransactionEventRaw> input,
            ExternalConfiguration config,
            boolean logLatencies
            ) {

        return AsyncDataStream.unorderedWait(
                input,
                new CassandraEnrichmentAsyncCache(
                        name,
                        logLatencies,
                        config.getDbHost(),
                        config.getDbPort(),
                        config.getDbUser(),
                        config.getDbPassword(),
                        config.getDbKeyspace(),
                        config.getCacheMaxSize()
                ),
                config.getAsyncTimeOutSec(),
                TimeUnit.SECONDS,
                config.getAsyncCapacity()
        ).name("Async-Cache-Enrichment");
    }

    private static DataStream<TransactionEventRaw> runSyncEnrichment(
            String name,
            DataStream<TransactionEventRaw> input,
            ExternalConfiguration config,
            boolean logLatencies
            ) {
        return input.process(
                new CassandraEnrichmentSync(
                        name,
                        logLatencies,
                        config.getDbHost(),
                        config.getDbPort(),
                        config.getDbUser(),
                        config.getDbPassword(),
                        config.getDbKeyspace()
                )
        ).name("Sync-Enrichment");
    }

}
