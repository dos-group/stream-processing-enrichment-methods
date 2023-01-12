package org.myorg.Events.TransactionEvent;

import org.myorg.Cache.TransactionCache;

import java.util.Objects;

public class TransactionEvent extends EventBase {

    private String accountId;
    private String result;
    private String recipientId;
    private AmountEvent amount;

    public TransactionEvent() {
        this(null, null, null, null, null, 0L);
    }

    public TransactionEvent(
            String accountId,
            String recipientId,
            String transactionId,
            AmountEvent amount,
            String result,
            Long timestamp
    ) {
        this.accountId = accountId;
        this.result = result;
        this.recipientId = recipientId;
        this.amount = amount;
        setTransactionId(transactionId);
        setTimestamp(timestamp);
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getRecipientId() {
        return recipientId;
    }

    public void setRecipientId(String recipientId) {
        this.recipientId = recipientId;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public AmountEvent getAmount() {
        return amount;
    }

    public void setAmount(AmountEvent amount) {
        this.amount = amount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionEvent that = (TransactionEvent) o;
        return Objects.equals(accountId, that.accountId) &&
                Objects.equals(result, that.result) &&
                Objects.equals(recipientId, that.recipientId) &&
                Objects.equals(amount, that.amount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accountId, result, "0", recipientId, amount);
    }

    @Override
    public SuspiciousType getSuspiciousType() {
        return SuspiciousType.UNKNOWN_RECIPIENT;
    }

    @Override
    public String getCacheKey() {
        return TransactionCache.getKey(this);
    }

    @Override
    public String toString() {
        return "TransactionEvent{" +
                "accountId='" + accountId + '\'' +
                ", result='" + result + '\'' +
                ", recipientId='" + recipientId + '\'' +
                ", amount=" + amount + '\'' +
                ", transactionId=" + getTransactionId() +
                '}';
    }
}
