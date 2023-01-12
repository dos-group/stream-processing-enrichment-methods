package org.myorg.EnrichmentMethods;

import com.datastax.driver.core.*;
import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.java.tuple.Tuple3;
import org.myorg.Events.TransactionEvent.EventBase;
import org.myorg.Events.TransactionEvent.EventFactory;
import org.myorg.Events.TransactionEvent.SuspiciousType;
import org.myorg.Events.TransactionEvent.TransactionEventRaw;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EnrichmentHelper {

    public static Session getClient(
            String host,
            int port,
            String user,
            String password,
            String keyspace
    ) {
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions
                .setCoreConnectionsPerHost(HostDistance.LOCAL,  2)
                .setMaxConnectionsPerHost(HostDistance.LOCAL, 10)
                .setCoreConnectionsPerHost(HostDistance.REMOTE, 2)
                .setMaxConnectionsPerHost(HostDistance.REMOTE, 10)
                .setMaxRequestsPerConnection(HostDistance.REMOTE, 1000)
                .setMaxRequestsPerConnection(HostDistance.LOCAL, 1000);

        return Cluster.builder()
                .addContactPoint(host)
                .withPort(port)
                .withCredentials(user, password)
                .withPoolingOptions(poolingOptions)
                .build()
                .connect(keyspace);
    }

    public static void printLatencies(String name, TransactionEventRaw input, long startQueryTime) {
        long stopTime = System.currentTimeMillis();
        long queryTime = stopTime - startQueryTime;
        System.out.printf(
                "%s QUERY TIME: (1) %s (2) %s%n",
                name,
                queryTime,
                (stopTime - input.getTransaction().getTimestamp())
        );
    }

    public static void LogSuspiciousTypeResult(
            String name,
            TransactionEventRaw input,
            SuspiciousType suspiciousType
    ) {
        Log.info(String.format(
                "%s ENRICHMENT account: %s has result for suspicious type: %s",
                name,
                input.getTransaction().getAccountId(),
                suspiciousType
        ));
    }

    public static TransactionEventRaw getEnrichedTransaction(
            TransactionEventRaw input,
            Tuple3<ResultSet, ResultSet, ResultSet> resultSets
    ) {
        ArrayList<ResultSet> resultSetsList = new ArrayList<>(
                Arrays.asList(resultSets.f0, resultSets.f1, resultSets.f2)
        );
        for (ResultSet resultSet : resultSetsList) {
            if (resultSet != null) {
                List<Row> rows = resultSet.all();
                input = enrichTransaction(input, resultSet, rows);
            }
        }
        return input;
    }

    public static <T> TransactionEventRaw enrichTransaction(
            TransactionEventRaw input,
            EventBase enrichment
    ) {
        SuspiciousType suspiciousType = enrichment.getSuspiciousType();
        input.suspiciousTypes.set(suspiciousType.ordinal(), suspiciousType);
        return input;
    }

    public static TransactionEventRaw enrichTransaction(
            TransactionEventRaw input,
            ResultSet enrichment,
            List<Row> rows
    ) {
        SuspiciousType suspiciousType = getSuspiciousTypeByTableName(enrichment);
        if (rows.size() <= 0) {
            input.suspiciousTypes.set(suspiciousType.ordinal(), suspiciousType);
        }
        return input;
    }

    public static TransactionEventRaw enrichTransaction(
            TransactionEventRaw input,
            SuspiciousType suspiciousType
    ) {
        input.suspiciousTypes.set(suspiciousType.ordinal(), suspiciousType);
        return input;
    }

    public static <T> SuspiciousType getSuspiciousTypeByCacheType(T enrichment) {
        if (enrichment instanceof EventBase) {
            return ((EventBase) enrichment).getSuspiciousType();
        } else {
            return null;
        }
    }

    public static SuspiciousType getSuspiciousTypeByTableName(ResultSet resultSet) {
        EventBase event = EventFactory.makeEvent(resultSet, null);
        return event != null ? event.getSuspiciousType() : null;
    }

    public static SuspiciousType getSuspiciousType(String tableName) {
        switch (tableName) {
            case "transactions": return SuspiciousType.UNKNOWN_RECIPIENT;
            case "devices": return SuspiciousType.UNKNOWN_DEVICE;
            case "locations": return SuspiciousType.UNKNOWN_LOCATION;
            default: return null;
        }
    }

}
