package org.myorg.Events.TransactionEvent;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.List;

public class EventFactory {

    public static <T> T makeEvent(String value, Class<T> eventClass) {
        Gson gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .create();
        return gson.fromJson(value, eventClass);
    }

    public static String toString(EventBase eventBase) {
        Gson gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .enableComplexMapKeySerialization()
                .create();

        return gson.toJson(eventBase);
    }

    public static EventBase makeEvent(ResultSet resultSet, List<Row> rows) {
        String tableName = resultSet.getColumnDefinitions().asList().get(0).getTable();
        Row row = rows != null && rows.size() > 0 ? rows.get(0) : null;

        switch (tableName) {
            case "transactions":
                return getTransactionEvent(row);
            case "devices":
                return getDeviceEvent(row);
            case "locations":
                return getLocationEvent(row);
            default: return null;
        }
    }

    private static TransactionEvent getTransactionEvent(Row row) {
        if (row == null) {
            return new TransactionEvent();
        } else {
            return new TransactionEvent(
                    row.getString("account_id"),
                    row.getString("recipient_id"),
                    row.getString("transaction_id"),
                    new AmountEvent(
                        row.getString("currency"),
                        row.getInt("amount_precision"),
                        row.getInt("amount_value")
                    ),
                    row.getString("result"),
                    0L
            );
        }
    }

    private static DeviceEvent getDeviceEvent(Row row) {
        if (row == null) {
            return new DeviceEvent();
        } else {
           return new DeviceEvent(
                    row.getString("account_id"),
                    row.getString("hash"),
                    row.getString("os_name"),
                    row.getString("os_version"),
                    row.getString("browser_name"),
                    row.getString("browser_version"),
                    row.getString("timezone"),
                    row.getString("language")
            );
        }
    }

    private static LocationEvent getLocationEvent(Row row) {
        if (row == null) {
            return new LocationEvent();
        } else {
            return new LocationEvent(
                    row.getString("account_id"),
                    row.getString("hash"),
                    row.getDouble("altitude"),
                    row.getDouble("longitude"),
                    row.getDouble("latitude")
            );
        }
    }

}
