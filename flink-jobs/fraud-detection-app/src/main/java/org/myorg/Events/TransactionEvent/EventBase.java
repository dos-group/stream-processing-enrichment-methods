package org.myorg.Events.TransactionEvent;

public abstract class EventBase {
    private Long timestamp;
    private String transactionId;
    private EventBase cachedEntry;

    public abstract SuspiciousType getSuspiciousType();
    public abstract String getCacheKey();

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public EventBase getCachedEntry() {
        return cachedEntry;
    }

    public void setCachedEntry(EventBase cachedEntry) {
        this.cachedEntry = cachedEntry;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }
}
