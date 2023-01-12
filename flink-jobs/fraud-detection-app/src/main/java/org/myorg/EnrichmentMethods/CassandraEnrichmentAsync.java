package org.myorg.EnrichmentMethods;

import com.datastax.driver.core.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.myorg.Events.TransactionEvent.TransactionEventRaw;

public class CassandraEnrichmentAsync extends CassandraEnrichmentAsyncBase {

    public CassandraEnrichmentAsync(
            String name,
            boolean logLatencies,
            String host,
            int port,
            String user,
            String password,
            String keyspace
    ) {
        super(name, logLatencies, host, port, user, password, keyspace);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        client = EnrichmentHelper.getClient(host, port, user, password, keyspace);
    }

    @Override
    public void asyncInvoke(
            TransactionEventRaw input,
            ResultFuture<TransactionEventRaw> resultFuture
    ) {
        long startTime = System.currentTimeMillis();

        String recipientQuery = CassandraQueryManager.getRecipientQuery(input);
        ResultSetFuture resultSetFutureRecipient = client.executeAsync(recipientQuery);

        String deviceQuery = CassandraQueryManager.getDeviceQuery(input);
        ResultSetFuture resultSetFutureDevice = client.executeAsync(deviceQuery);

        String locationQuery = CassandraQueryManager.getLocationQuery(input);
        ResultSetFuture resultSetFutureLocation = client.executeAsync(locationQuery);

        applyAsync(
                startTime,
                input,
                new Tuple3<>(resultSetFutureRecipient, resultSetFutureDevice, resultSetFutureLocation),
                resultFuture
        );
    }

}

