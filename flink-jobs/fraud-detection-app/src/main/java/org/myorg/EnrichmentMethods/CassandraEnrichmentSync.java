package org.myorg.EnrichmentMethods;

import com.datastax.driver.core.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.myorg.Events.TransactionEvent.TransactionEventRaw;

import java.util.ArrayList;
import java.util.List;

public class CassandraEnrichmentSync extends ProcessFunction<TransactionEventRaw, TransactionEventRaw> {

    private Session client;
    private final String host;
    private final String user;
    private final String password;
    private final String keyspace;
    private final int port;

    private final String name;
    private final boolean logLatencies;

    public CassandraEnrichmentSync(
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
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        client = EnrichmentHelper.getClient(host, port, user, password, keyspace);
    }

    /**
     * Process one element from the input stream.
     *
     * <p>This function can output zero or more elements using the {@link Collector} parameter and
     * also update internal state or set timers using the {@link Context} parameter.
     *
     * @param input The input value.
     * @param ctx   A {@link Context} that allows querying the timestamp of the element and getting a
     *              {@link TimerService} for registering timers and querying the time. The context is only
     *              valid during the invocation of this method, do not store it.
     * @param out   The collector for returning result values.
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *                   operation to fail and may trigger recovery.
     */
    @Override
    public void processElement(
            TransactionEventRaw input,
            Context ctx,
            Collector<TransactionEventRaw> out
    ) throws Exception {
        long startTime = System.currentTimeMillis();
        ArrayList<ResultSet> resultSets = new ArrayList<>();

        String recipientQuery = CassandraQueryManager.getRecipientQuery(input);
        resultSets.add(client.execute(recipientQuery));

        String deviceQuery = CassandraQueryManager.getDeviceQuery(input);
        resultSets.add(client.execute(deviceQuery));

        String locationQuery = CassandraQueryManager.getLocationQuery(input);
        resultSets.add(client.execute(locationQuery));

        for (ResultSet resultSet : resultSets) {
            List<Row> rows = resultSet.all();
            input = EnrichmentHelper.enrichTransaction(input, resultSet, rows);
        }

        if (logLatencies)
            EnrichmentHelper.printLatencies(name, input, startTime);

        out.collect(input);
    }
}
