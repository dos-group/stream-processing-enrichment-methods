package org.myorg.Cache;

import org.myorg.Events.LogEvent.ModelSession;
import org.myorg.Events.TransactionEvent.EventBase;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

public class ModelSessionCache implements Serializable {

    public transient Map<String, ModelSession> cache;

    public ModelSessionCache(int maxSize, int idx) {
        cache = Collections.synchronizedMap(new LRUCache<>(maxSize, idx));
    }

    public ModelSession getOldestEntry() {
        if (cache.size() > 0)
            return (ModelSession) cache.entrySet().toArray()[cache.size() - 1];
        else
            return null;
    }

    public ArrayList<String> getKeys() {
        ArrayList<String> keys = new ArrayList<>();
        for (Map.Entry<String, ModelSession> entry : cache.entrySet())
            keys.add(entry.getKey());
        return keys;
    }

    public void add(String key, ModelSession modelSession) {
        cache.put(key, modelSession);
    }

    public boolean containsKey(String key) {
        return cache.containsKey(key);
    }

    public ModelSession get(String key) {
        return cache.get(key);
    }

    public void clear() {
        cache.clear();
    }

}
