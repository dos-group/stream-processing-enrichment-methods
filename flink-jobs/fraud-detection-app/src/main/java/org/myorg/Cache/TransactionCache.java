package org.myorg.Cache;

import org.myorg.Events.TransactionEvent.DeviceEvent;
import org.myorg.Events.TransactionEvent.EventBase;
import org.myorg.Events.TransactionEvent.LocationEvent;
import org.myorg.Events.TransactionEvent.TransactionEvent;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

public class TransactionCache implements Serializable {

    public transient Map<String, EventBase> cache;

    public TransactionCache(int maxSize, int idx) {
        this.cache = Collections.synchronizedMap(new LRUCache<>(maxSize, idx));
    }

    public void addTransaction(TransactionEvent transactionEvent) {
        cache.put(getRecipientTransactionKey(transactionEvent), transactionEvent);
    }

    public void addDevice(DeviceEvent deviceEvent) {
        cache.put(getDeviceKey(deviceEvent), deviceEvent);
    }

    public void addLocation(LocationEvent locationEvent) {
        cache.put(getLocationKey(locationEvent), locationEvent);
    }

    public void add(EventBase eventBase) {
        if (eventBase instanceof TransactionEvent) {
            addTransaction((TransactionEvent) eventBase);
        } else if (eventBase instanceof DeviceEvent) {
            addDevice((DeviceEvent) eventBase);
        } else if (eventBase instanceof LocationEvent) {
            addLocation((LocationEvent) eventBase);
        }
    }

    public TransactionEvent getRecipientTransaction(TransactionEvent transactionEvent) {
        String key = getRecipientTransactionKey(transactionEvent);
        EventBase value = cache.get(key);
        return value != null ? (TransactionEvent) value : null;
    }

    public DeviceEvent getDevice(DeviceEvent deviceEvent) {
        String key = getDeviceKey(deviceEvent);
        EventBase value = cache.get(key);
        return value != null ? (DeviceEvent) value : null;
    }

    public LocationEvent getLocation(LocationEvent locationEvent) {
        String key = getLocationKey(locationEvent);
        EventBase value = cache.get(key);
        return value != null ? (LocationEvent) value : null;
    }

    public static String getKey(EventBase eventBase) {
        if (eventBase instanceof TransactionEvent) {
            return getRecipientTransactionKey((TransactionEvent) eventBase);
        } else if (eventBase instanceof DeviceEvent) {
            return getDeviceKey((DeviceEvent) eventBase);
        } else if (eventBase instanceof LocationEvent) {
            return getLocationKey((LocationEvent) eventBase);
        } else {
            return null;
        }
    }

    public static String getRecipientTransactionKey(TransactionEvent transactionEvent) {
        return transactionEvent.getAccountId() + transactionEvent.getRecipientId();
    }

    public static String getDeviceKey(DeviceEvent deviceEvent) {
        return deviceEvent.getAccountId() + deviceEvent.getHash();
    }

    public static String getLocationKey(LocationEvent locationEvent) {
        return locationEvent.getAccountId() + locationEvent.getHash();
    }

}
