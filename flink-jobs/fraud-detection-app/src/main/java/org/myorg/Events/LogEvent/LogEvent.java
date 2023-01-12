package org.myorg.Events.LogEvent;

import org.myorg.EnrichmentMethods.ModelRaw;

public class LogEvent {

    private String key;
    private ModelRaw modelRaw;
    private long timestamp, windowStartTimestamp, windowEndTimestamp;


    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setModelRaw(ModelRaw modelRaw) {
        this.modelRaw = modelRaw;
    }

    public ModelRaw getModelRaw() {
        return modelRaw;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getWindowStartTimestamp() {
        return windowStartTimestamp;
    }

    public void setWindowStartTimestamp(long windowStartTimestamp) {
        this.windowStartTimestamp = windowStartTimestamp;
    }

    public long getWindowEndTimestamp() {
        return windowEndTimestamp;
    }

    public void setWindowEndTimestamp(long windowEndTimestamp) {
        this.windowEndTimestamp = windowEndTimestamp;
    }
}
