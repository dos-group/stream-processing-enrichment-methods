package org.myorg.EnrichmentMethods;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.myorg.Events.TransactionEvent.TransactionEventRaw;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public abstract class CassandraEnrichmentAsyncBase
        extends RichAsyncFunction<TransactionEventRaw, TransactionEventRaw> {

    public Session client;
    public final String host;
    public final String user;
    public final String password;
    public final String keyspace;
    public final int port;

    public final String name;
    public final boolean logLatencies;

    public CassandraEnrichmentAsyncBase (
            String name,
            boolean logLatencies,
            String host,
            int port,
            String user,
            String password,
            String keyspace
    ) {
        super();
        this.name = name;
        this.logLatencies = logLatencies;
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.keyspace = keyspace;
    }

    @Override
    public abstract void asyncInvoke(
            TransactionEventRaw input,
            ResultFuture<TransactionEventRaw> resultFuture
    ) throws Exception;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    public void applyAsync(
            long startTime,
            TransactionEventRaw input,
            Tuple3<ResultSetFuture, ResultSetFuture, ResultSetFuture> resultSets,
            ResultFuture<TransactionEventRaw> resultFuture
    ) {
//        CompletableFuture.supplyAsync(
//                new CassandraEnrichmentAsyncSupplier(resultSets)
//        ).thenAcceptAsync((Tuple3<ResultSet, ResultSet, ResultSet> res) -> {
//            if (logLatencies)
//                EnrichmentHelper.printLatencies(name, input, startTime);
//
//            TransactionEventRaw out = EnrichmentHelper.getEnrichedTransaction(input, res);
//            resultFuture.complete(Collections.singleton(out));
//        });
        CompletableFuture.supplyAsync(
                new CassandraEnrichmentAsyncSupplier(resultSets)
        ).thenAcceptAsync((Tuple3<ResultSet, ResultSet, ResultSet> res) -> {
            if (logLatencies)
                EnrichmentHelper.printLatencies(name, input, startTime);

            ArrayList<ResultSet> resultSetsList = new ArrayList<>(
                    Arrays.asList(res.f0, res.f1, res.f2)
            );
            TransactionEventRaw out = input;
            for (ResultSet resultSet : resultSetsList) {
                if (resultSet == null)
                    continue;
                List<Row> rows = resultSet.all();
                out = EnrichmentHelper.enrichTransaction(input, resultSet, rows);
//                List<Row> rows = resultSet.all();
//                if (rows.size() <= 0) {
//                    EventBase eventBase = EventFactory.makeEvent(resultSet, rows);
//                    if (eventBase != null) {
//                        SuspiciousType suspiciousType = eventBase.getSuspiciousType();
//                        input.suspiciousTypes.set(suspiciousType.ordinal(), suspiciousType);
//                    }
//                }
            }
            resultFuture.complete(Collections.singleton(out));
        });
    }

}
