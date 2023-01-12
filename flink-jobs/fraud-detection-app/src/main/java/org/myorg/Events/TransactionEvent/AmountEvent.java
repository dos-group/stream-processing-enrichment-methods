package org.myorg.Events.TransactionEvent;

import java.io.Serializable;
import java.util.Objects;

public class AmountEvent implements Serializable {
    private String currency;
    private int precision;
    private int value;

    public AmountEvent() {}

    public AmountEvent(
            String currency,
            int precision,
            int value
    ) {
        this.currency = currency;
        this.precision = precision;
        this.value = value;
    }

    public AmountEvent(AmountEvent amountEvent) {
        this.currency = amountEvent.currency;
        this.precision = amountEvent.precision;
        this.value = amountEvent.value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AmountEvent that = (AmountEvent) o;
        return precision == that.precision &&
                value == that.value &&
                Objects.equals(currency, that.currency);
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public int getPrecision() {
        return precision;
    }

    public  void setPrecision(int precision) {
        this.precision = precision;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }
}
