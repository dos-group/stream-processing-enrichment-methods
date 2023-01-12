package org.myorg.Cache;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private final int maxSize;
    private final int idx;

    public LRUCache(int maxSize, int idx) {
        super(maxSize, 0.75f, true);
        this.maxSize = maxSize;
        this.idx = idx;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > maxSize;
    }

}
