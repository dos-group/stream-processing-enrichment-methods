package org.myorg.Events.LogEvent;

public class LogEventResult {

    private String key, result;
    private float[] out;
    private long windowStart, windowEnd;

    public LogEventResult(String key, float[] out, long windowStart, long windowEnd) {
        this.key = key;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.out = out;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(long windowStart) {
        this.windowStart = windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public float[] getOut() {
        return out;
    }

    public void setOut(float[] out) {
        this.out = out;
    }
}
