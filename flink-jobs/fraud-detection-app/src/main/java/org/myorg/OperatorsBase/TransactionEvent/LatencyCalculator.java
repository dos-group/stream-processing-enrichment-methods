package org.myorg.OperatorsBase.TransactionEvent;

import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.util.Collector;
import org.myorg.Events.TransactionEvent.TransactionEventRaw;
import org.myorg.Events.TransactionEvent.TransactionResult;

import java.sql.Timestamp;
import java.util.*;

public class LatencyCalculator extends RichFlatMapFunction<TransactionResult, TransactionResult> {
    private final int windowSizeSeconds;
    private final Map<Long, Map<String, TransactionEventRaw>> cachedWindowTransactions;
    private transient Histogram histogram;

    public LatencyCalculator(int windowSizeSeconds) {
        this.windowSizeSeconds = windowSizeSeconds;
        this.cachedWindowTransactions = new HashMap<>();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        com.codahale.metrics.Histogram dropwizardHistogram =
                new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));
        this.histogram = getRuntimeContext()
                .getMetricGroup()
                .histogram("myLatencyHistogram", new DropwizardHistogramWrapper(dropwizardHistogram));
    }

    @Override
    public void flatMap(TransactionResult value, Collector<TransactionResult> out) throws Exception {
        Map<String, TransactionEventRaw> currWindowTransactions = new HashMap<>();
        for (TransactionEventRaw transactionEventRaw : value.getWindowTransactions()) {
            if (isLatencyAlreadyAdded(transactionEventRaw))  {
                continue;
            }
            long latency = getLatency(transactionEventRaw);
//            System.out.println("LATENCY: " + latency);
            histogram.update(latency);
            currWindowTransactions.put(transactionEventRaw.getTransaction().getTransactionId(), transactionEventRaw);
        }

        // delete irrelevant window transactions
        long msToRemove = (long) windowSizeSeconds * 2 * 1000;
        Timestamp relevantTransactionsStart = new Timestamp(System.currentTimeMillis() - msToRemove);
        cachedWindowTransactions.keySet().removeIf(windowStart -> {
            Timestamp currWindowStartTs = new Timestamp(windowStart);
            return currWindowStartTs.before(relevantTransactionsStart);
        });

        // add transactions
        if (cachedWindowTransactions.containsKey(value.getWindowStart())) {
            cachedWindowTransactions.get(value.getWindowStart()).putAll(currWindowTransactions);
        } else {
            cachedWindowTransactions.put(value.getWindowStart(), currWindowTransactions);
        }

        value.clearWindowTransactions();
        out.collect(value);
    }

    private boolean isLatencyAlreadyAdded(TransactionEventRaw transactionEventRaw) {
        for (Map<String, TransactionEventRaw> window : cachedWindowTransactions.values()) {
            if (window.containsKey(transactionEventRaw.getTransaction().getTransactionId())) {
                return true;
            }
        }
        return false;
    }

    private Long getLatency(TransactionEventRaw transactionEventRaw) {
        long totalDuration = System.currentTimeMillis() - transactionEventRaw.getTransaction().getTimestamp();
        long windowDuration = transactionEventRaw.getWindowEndMeasure() - transactionEventRaw.getWindowStartMeasure();
        return totalDuration - windowDuration;
    }

}
