package org.myorg.EnrichmentMethods;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class CassandraEnrichmentAsyncSupplier implements Supplier<Tuple3<ResultSet, ResultSet, ResultSet>> {

    private final Tuple3<ResultSetFuture, ResultSetFuture, ResultSetFuture> resultSets;

    public CassandraEnrichmentAsyncSupplier(
            Tuple3<ResultSetFuture, ResultSetFuture, ResultSetFuture> resultSets
    ) {
        this.resultSets = resultSets;
    }

    @Override
    public Tuple3<ResultSet, ResultSet, ResultSet> get() {
        try {
            return new Tuple3<>(
                    resultSets.f0 == null ? null : resultSets.f0.get(),
                    resultSets.f1 == null ? null : resultSets.f1.get(),
                    resultSets.f2 == null ? null : resultSets.f2.get()
            );
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }
}
