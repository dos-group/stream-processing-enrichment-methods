package org.myorg.EnrichmentMethods;

import com.codahale.metrics.SlidingWindowReservoir;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.myorg.Events.TransactionEvent.*;
import org.myorg.Cache.TransactionCache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class CassandraEnrichmentAsyncCacheRedis extends CassandraEnrichmentAsyncBase {

    private RedisAsyncCommands<String, String> redisAsyncCommands;
    private final String redisHost;
    private final int redisPort;
    private final String redisPassword;
    private final String redisHashKey;

    public transient Counter totalExternalAccess, cacheHitMetric;
    public transient Histogram cacheHitHistogram, cacheSize;
    private int cacheAddCounter;

    public CassandraEnrichmentAsyncCacheRedis(
            String name,
            boolean logLatencies,
            String host,
            int port,
            String user,
            String password,
            String keyspace,
            String redisHost,
            int redisPort,
            String redisPassword,
            String redisHashKey
    ) {
        super(name, logLatencies, host, port, user, password, keyspace);
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisPassword = redisPassword;
        this.redisHashKey = redisHashKey;
        this.cacheAddCounter = 0;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        client = EnrichmentHelper.getClient(host, port, user, password, keyspace);

        RedisClient redisClient = RedisClient.create(
                String.format("redis://%s@%s:%d/", redisPassword, redisHost, redisPort)
        );
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        redisAsyncCommands = connection.async();

        totalExternalAccess = getRuntimeContext().getMetricGroup().counter("totalEvents");
        cacheHitMetric = getRuntimeContext().getMetricGroup().counter("cacheHitMetric");

        com.codahale.metrics.Histogram dropwizardHistogram =
                new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));
        cacheHitHistogram = getRuntimeContext()
                .getMetricGroup()
                .histogram("cacheHitHistogram", new DropwizardHistogramWrapper(dropwizardHistogram));

        com.codahale.metrics.Histogram dropwizardHistogram2 =
                new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));
        cacheSize = getRuntimeContext()
                .getMetricGroup()
                .histogram("cacheSizeHistogram", new DropwizardHistogramWrapper(dropwizardHistogram2));
    }

    @Override
    public void asyncInvoke(
            TransactionEventRaw input,
            ResultFuture<TransactionEventRaw> resultFuture
    ) {
        long startTime = System.currentTimeMillis();
        ResultSetFuture resultSetFutureRecipient = null, resultSetFutureDevice = null, resultSetFutureLocation = null;

        TransactionEvent knownRecipientTransaction = (TransactionEvent) input.getTransaction().getCachedEntry();
        totalExternalAccess.inc();
        if (knownRecipientTransaction != null) {
            input = EnrichmentHelper.enrichTransaction(input, knownRecipientTransaction);
            cacheHitMetric.inc();
        } else {
            String recipientQuery = CassandraQueryManager.getRecipientQuery(input);
            resultSetFutureRecipient = client.executeAsync(recipientQuery);
        }

        DeviceEvent knownDevice = (DeviceEvent) input.getDevice().getCachedEntry();
        totalExternalAccess.inc();
        if (knownDevice != null) {
            input = EnrichmentHelper.enrichTransaction(input, knownDevice);
            cacheHitMetric.inc();
        } else {
            String deviceQuery = CassandraQueryManager.getDeviceQuery(input);
            resultSetFutureDevice = client.executeAsync(deviceQuery);
        }

        LocationEvent knownLocation = (LocationEvent) input.getLocation().getCachedEntry();
        totalExternalAccess.inc();
        if (knownLocation != null) {
            input = EnrichmentHelper.enrichTransaction(input, knownLocation);
            cacheHitMetric.inc();
        } else {
            String locationQuery = CassandraQueryManager.getLocationQuery(input);
            resultSetFutureLocation = client.executeAsync(locationQuery);
        }

        int cacheHitRate = (int) (((float) cacheHitMetric.getCount() / (float) totalExternalAccess.getCount())  * 100);
        cacheHitHistogram.update(cacheHitRate);
//        System.out.println("Cache Hit Rate: " + cacheHitRate);

        if (resultSetFutureRecipient != null || resultSetFutureDevice != null || resultSetFutureLocation != null) {
            TransactionEventRaw finalInput = input;
            CompletableFuture.supplyAsync(
                    new CassandraEnrichmentAsyncSupplier(
                            new Tuple3<>(resultSetFutureRecipient, resultSetFutureDevice, resultSetFutureLocation)
                    )
            ).thenAcceptAsync((Tuple3<ResultSet, ResultSet, ResultSet> res) -> {
                if (logLatencies)
                    EnrichmentHelper.printLatencies(name, finalInput, startTime);

                ArrayList<ResultSet> resultSetsList = new ArrayList<>(
                        Arrays.asList(res.f0, res.f1, res.f2)
                );
                TransactionEventRaw out = finalInput;
                for (ResultSet resultSet : resultSetsList) {
                    if (resultSet == null)
                        continue;
                    List<Row> rows = resultSet.all();
                    if (rows.size() > 0) {
                        EventBase eventBase = EventFactory.makeEvent(resultSet, rows);
                        if (eventBase != null) {
                            redisAsyncCommands.hset(redisHashKey, TransactionCache.getKey(eventBase), EventFactory.toString(eventBase));
                            cacheSize.update(++cacheAddCounter);
                        }
                    } else {
                        out = EnrichmentHelper.enrichTransaction(finalInput, resultSet, rows);
                    }
                }
                resultFuture.complete(Collections.singleton(out));
            });
        } else {
            resultFuture.complete(Collections.singleton(input));
        }
    }
}
