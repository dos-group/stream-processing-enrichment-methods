package org.myorg.OperatorsBase.TransactionEvent;

import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.myorg.Events.TransactionEvent.SuspiciousTransaction;
import org.myorg.Events.TransactionEvent.SuspiciousType;
import org.myorg.Events.TransactionEvent.TransactionEventRaw;
import org.myorg.Events.TransactionEvent.TransactionResult;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class TransactionWindow extends RichWindowFunction<TransactionEventRaw, TransactionResult, String, TimeWindow> {

    private final HashMap<String, List<TransactionEventRaw>> windowTransactions = new HashMap<>();

    /**
     * Evaluates the window and outputs none or several elements.
     *
     * @param s      The key for which this window is evaluated.
     * @param window The window that is being evaluated.
     * @param input  The elements in the window being evaluated.
     * @param out    A collector for emitting elements.
     * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
     */
    @Override
    public void apply(
            String s,
            TimeWindow window,
            Iterable<TransactionEventRaw> input,
            Collector<TransactionResult> out
    ) throws Exception {
        int amountSum = 0, transactionCnt = 0;
        String key = null;
        boolean isWindowDifferent = false;
        List<TransactionEventRaw> transactions = new LinkedList<>();
        List<SuspiciousTransaction> suspiciousTransactions = new LinkedList<>();

        for (TransactionEventRaw e : input) {
            e.setWindowEndMeasure(System.currentTimeMillis());
            transactions.add(e);

            transactionCnt++;
            amountSum += e.getTransaction().getAmount().getValue();
            key = e.getKey();

            if (isSuspiciousTransaction(e)) {
                suspiciousTransactions.add(
                        new SuspiciousTransaction(e.getTransaction().getTransactionId(), e.suspiciousTypes)
                );
            }

            // check if there are new events in window
            if (windowTransactions.containsKey(key) && !windowTransactions.get(key).contains(e)) {
                isWindowDifferent = true;
            }
        }

        // safe events if key does not exist
        if (!windowTransactions.containsKey(key)) {
            windowTransactions.put(key, transactions);
            // replace events if events changed in window
        } else if (windowTransactions.containsKey(key) && isWindowDifferent) {
            windowTransactions.replace(key, transactions);
            // return if no new events occurred
        } else {
            return;
        }

        TransactionResult transactionResult = new TransactionResult(
                key,
                window.getStart(),
                window.maxTimestamp(),
                amountSum,
                transactionCnt,
                suspiciousTransactions
        );
        transactionResult.setWindowTransactions(transactions);

        out.collect(transactionResult);
    }

    private boolean isSuspiciousTransaction(TransactionEventRaw transactionEventRaw) {
        for (SuspiciousType suspiciousType : transactionEventRaw.suspiciousTypes) {
            if (suspiciousType != SuspiciousType.NONE)
                return true;
        }
        return false;
    }
}
