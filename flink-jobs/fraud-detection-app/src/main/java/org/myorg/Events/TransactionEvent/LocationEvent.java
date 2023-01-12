package org.myorg.Events.TransactionEvent;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import org.myorg.Cache.TransactionCache;

@Table(keyspace = "master", name = "locations")
public class LocationEvent extends EventBase {

    @Column(name = "account_id")
    private String accountId = "";

    @Column(name = "hash")
    private String hash = "";

    @Column(name = "altitude")
    private double altitude = 0.0;

    @Column(name = "longitude")
    private double longitude = 0.0;

    @Column(name = "latitude")
    private double latitude = 0.0;

    public LocationEvent() {}

    public LocationEvent(
            String accountId,
            String hash,
            double altitude,
            double longitude,
            double latitude
    ) {
        this.accountId = accountId;
        this.hash = hash;
        this.altitude = altitude;
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getAccountId() {
        return accountId;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public void setAltitude(double altitude) {
        this.altitude = altitude;
    }

    public double getAltitude() {
        return altitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLatitude() {
        return latitude;
    }

    @Override
    public String toString() {
        return "LocationSink{" +
                "accountId='" + accountId + '\'' +
                ", hash='" + hash + '\'' +
                ", altitude=" + altitude +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                '}';
    }

    @Override
    public SuspiciousType getSuspiciousType() {
        return SuspiciousType.UNKNOWN_LOCATION;
    }

    @Override
    public String getCacheKey() {
        return TransactionCache.getKey(this);
    }
}
