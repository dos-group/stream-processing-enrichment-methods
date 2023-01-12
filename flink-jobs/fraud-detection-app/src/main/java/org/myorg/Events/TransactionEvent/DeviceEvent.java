package org.myorg.Events.TransactionEvent;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import org.myorg.Cache.TransactionCache;

@Table(keyspace = "master", name = "devices")
public class DeviceEvent extends EventBase {

    @Column(name = "account_id")
    private String accountId = "";

    @Column(name = "hash")
    private String hash = "";

    @Column(name = "os_name")
    private String osName = "";

    @Column(name = "os_version")
    private String osVersion = "";

    @Column(name = "browser_name")
    private String browserName = "";

    @Column(name = "browser_version")
    private String browserVersion = "";

    @Column(name = "timezone")
    private String timezone = "";

    @Column(name = "language")
    private String language = "";

    public DeviceEvent() {}

    public DeviceEvent(
            String accountId,
            String hash,
            String osName,
            String osVersion,
            String browserName,
            String browserVersion,
            String timezone,
            String language
    ) {
        this.accountId = accountId;
        this.hash = hash;
        this.osName = osName;
        this.osVersion = osVersion;
        this.browserName = browserName;
        this.browserVersion = browserVersion;
        this.timezone = timezone;
        this.language = language;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public String getHash() {
        return hash;
    }

    public void setOsName(String osName) {
        this.osName = osName;
    }

    public String getOsName() {
        return osName;
    }

    public void setOsVersion(String osVersion) {
        this.osVersion = osVersion;
    }

    public String getOsVersion() {
        return osVersion;
    }

    public void setBrowserName(String browserName) {
        this.browserName = browserName;
    }

    public String getBrowserName() {
        return browserName;
    }

    public void setBrowserVersion(String browserVersion) {
        this.browserVersion = browserVersion;
    }

    public String getBrowserVersion() {
        return browserVersion;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getLanguage() {
        return language;
    }

    @Override
    public SuspiciousType getSuspiciousType() {
        return SuspiciousType.UNKNOWN_DEVICE;
    }

    @Override
    public String getCacheKey() {
        return TransactionCache.getKey(this);
    }
}
