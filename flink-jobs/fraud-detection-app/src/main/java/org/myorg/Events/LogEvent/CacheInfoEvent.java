package org.myorg.Events.LogEvent;

public class CacheInfoEvent {

    private String key;
    private Integer isCached;

    public CacheInfoEvent(String key, Integer isCached) {
        this.key = key;
        this.isCached = isCached;
    }

    public CacheInfoEvent() {}

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Integer getIsCached() {
        return isCached;
    }

    public void setIsCached(Integer isCached) {
        this.isCached = isCached;
    }
}
