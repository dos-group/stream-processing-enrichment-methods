package org.myorg.Events.TransactionEvent;

import java.io.Serializable;
import java.util.*;

public class TransactionEventRaw implements Serializable {
    private TransactionEvent transaction;
    private LocationEvent location;
    private DeviceEvent device;
    private long windowStartMeasure, windowEndMeasure;

    public ArrayList<SuspiciousType> suspiciousTypes;

    public TransactionEventRaw() {
        this.suspiciousTypes = new ArrayList<>(
                Arrays.asList(SuspiciousType.NONE, SuspiciousType.NONE, SuspiciousType.NONE)
        );
    }

    public TransactionEventRaw(TransactionEventRaw transactionEventRaw) {
        this.transaction = transactionEventRaw.transaction;

        this.location = transactionEventRaw.location;
        this.location.setAccountId(this.transaction.getAccountId());
        this.location.setTimestamp(this.transaction.getTimestamp());
        this.location.setTransactionId(this.transaction.getTransactionId());

        this.device = transactionEventRaw.device;
        this.device.setAccountId(this.transaction.getAccountId());
        this.device.setTimestamp(this.transaction.getTimestamp());
        this.device.setTransactionId(this.transaction.getTransactionId());

        this.suspiciousTypes = new ArrayList<>(
                Arrays.asList(SuspiciousType.NONE, SuspiciousType.NONE, SuspiciousType.NONE)
        );
    }

    public String getKey() {
        return transaction.getAccountId();
    }

    public void setWindowStartMeasure(Long windowStartMeasure) {
        this.windowStartMeasure = windowStartMeasure;
    }

    public Long getWindowStartMeasure() {
        return windowStartMeasure;
    }

    public void setWindowEndMeasure(Long windowEndMeasure) {
        this.windowEndMeasure = windowEndMeasure;
    }

    public Long getWindowEndMeasure() {
        return windowEndMeasure;
    }

    public DeviceEvent getDevice() {
        return device;
    }

    public void setDevice(DeviceEvent device) {
        this.device = device;
    }

    public LocationEvent getLocation() {
        return location;
    }

    public void setLocation(LocationEvent location) {
        this.location = location;
    }

    public TransactionEvent getTransaction() {
        return transaction;
    }

    public void setTransaction(TransactionEvent transaction) {
        this.transaction = transaction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionEventRaw that = (TransactionEventRaw) o;
        return windowStartMeasure == that.windowStartMeasure &&
                windowEndMeasure == that.windowEndMeasure &&
                transaction.equals(that.transaction) &&
                location.equals(that.location) &&
                device.equals(that.device) &&
                suspiciousTypes.equals(that.suspiciousTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transaction, location, device, windowStartMeasure, windowEndMeasure, suspiciousTypes);
    }
}
