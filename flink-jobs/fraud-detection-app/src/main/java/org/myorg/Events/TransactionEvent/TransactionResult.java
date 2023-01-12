package org.myorg.Events.TransactionEvent;

import java.util.LinkedList;
import java.util.List;

public class TransactionResult {

    private final String accountId;
    private final long windowStart; // included in window
    private final long windowEnd; // included in window
    private final int transactionAmount;
    private final int transactionCnt;

    private final List<SuspiciousTransaction> suspiciousTransactions;
    private List<TransactionEventRaw> windowTransactions;

    public TransactionResult(
            String accountId,
            long windowStart,
            long windowEnd,
            int transactionAmount,
            int transactionCnt,
            List<SuspiciousTransaction> suspiciousTransactions
    ) {
        this.accountId = accountId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.transactionAmount = transactionAmount;
        this.transactionCnt = transactionCnt;
        this.suspiciousTransactions = suspiciousTransactions;
        windowTransactions = new LinkedList<>();
    }

    public int getTransactionAmount() {
        return transactionAmount;
    }

    public int getTransactionCnt() {
        return transactionCnt;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public String getAccountId() {
        return accountId;
    }

    public List<SuspiciousTransaction> getSuspiciousTransactions() {
        return suspiciousTransactions;
    }

    public List<TransactionEventRaw> getWindowTransactions() {
        return windowTransactions;
    }

    public void setWindowTransactions(List<TransactionEventRaw> windowTransactions) {
        this.windowTransactions = windowTransactions;
    }

    public void clearWindowTransactions() {
        windowTransactions.clear();
    }


    @Override
    public String toString() {
        return "TransactionResult{" +
                "accountId='" + accountId + '\'' +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                ", transactionAmount=" + transactionAmount +
                ", transactionCnt=" + transactionCnt +
                '}';
    }
}
