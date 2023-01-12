package org.myorg.EnrichmentMethods;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.myorg.Events.TransactionEvent.DeviceEvent;
import org.myorg.Events.TransactionEvent.LocationEvent;
import org.myorg.Events.TransactionEvent.TransactionEvent;
import org.myorg.Events.TransactionEvent.TransactionEventRaw;
import org.myorg.Events.TransactionEvent.EventFactory;
import org.myorg.Cache.TransactionCache;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class RedisCacheEnrichmentAsync extends RichAsyncFunction<TransactionEventRaw, TransactionEventRaw> {

    private final String host;
    private final int port;
    private final String password;
    private final String hashKey;
    private RedisAsyncCommands<String, String> redisAsyncCommands;

    public RedisCacheEnrichmentAsync(String host, int port, String password, String hashKey) {
        this.host = host;
        this.port = port;
        this.password = password;
        this.hashKey = hashKey;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        RedisClient redisClient = RedisClient.create(String.format("redis://%s@%s:%d/", password, host, port));
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        redisAsyncCommands = connection.async();
    }

    @Override
    public void asyncInvoke(
            TransactionEventRaw input,
            ResultFuture<TransactionEventRaw> resultFuture
    ) {
        long startTime = System.currentTimeMillis();

        String transactionKey = TransactionCache.getRecipientTransactionKey(input.getTransaction());
        String deviceKey = TransactionCache.getDeviceKey(input.getDevice());
        String locationKey = TransactionCache.getLocationKey(input.getLocation());

        RedisFuture<String> transactionFuture = redisAsyncCommands.hget(hashKey, transactionKey);
        RedisFuture<String> deviceFuture = redisAsyncCommands.hget(hashKey, deviceKey);
        RedisFuture<String> locationFuture = redisAsyncCommands.hget(hashKey, locationKey);

        CompletableFuture.supplyAsync(() -> {
            try {
                return new Tuple3<>(
                        transactionFuture.get(), //null, null
                        deviceFuture.get(),
                        locationFuture.get()
                );
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            return null;
        }).thenAcceptAsync(redisFutures -> {
//            EnrichmentHelper.printLatencies("REDIS-ASYNC", input, startTime);

            if (redisFutures.f0 != null) {
                TransactionEvent transactionEvent = EventFactory.makeEvent(redisFutures.f0, TransactionEvent.class);
                input.getTransaction().setCachedEntry(transactionEvent);
            }
            if (redisFutures.f1 != null) {
                DeviceEvent deviceEvent = EventFactory.makeEvent(redisFutures.f1, DeviceEvent.class);
                input.getDevice().setCachedEntry(deviceEvent);
            }
            if (redisFutures.f2 != null) {
                LocationEvent locationEvent = EventFactory.makeEvent(redisFutures.f2, LocationEvent.class);
                input.getLocation().setCachedEntry(locationEvent);
            }
            resultFuture.complete(Collections.singleton(input));
        });
    }


}
