package org.myorg.Events.TransactionEvent;

import java.util.LinkedList;
import java.util.List;


public class SuspiciousTransaction {
    private final String transactionId;
    private final List<SuspiciousType> types;

    public SuspiciousTransaction(String transactionId, List<SuspiciousType> types) {
        this.transactionId = transactionId;
        this.types = (types != null) ? types : new LinkedList<>();
    }

    public String getTransactionId() {
        return transactionId;
    }

    public List<SuspiciousType> getTypes() {
        return types;
    }

    @Override
    public String toString() {
        return "SuspiciousTransaction{" +
                "transactionId='" + transactionId + '\'' +
                ", type=" + types +
                '}';
    }
}
