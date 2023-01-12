package org.myorg.EnrichmentMethods;

import org.myorg.Events.TransactionEvent.TransactionEventRaw;

public class CassandraQueryManager {

    public static String getRecipientQuery(TransactionEventRaw transactionEventRaw) {
        return String.format(
                "select * from transactions where account_id='%s' and recipient_id='%s';",
                transactionEventRaw.getTransaction().getAccountId(),
                transactionEventRaw.getTransaction().getRecipientId()
        );
    }

    public static String getDeviceQuery(TransactionEventRaw transactionEventRaw) {
        return String.format(
                "select * from devices where account_id='%s' and hash='%s';",
                transactionEventRaw.getTransaction().getAccountId(),
                transactionEventRaw.getDevice().getHash()
        );
    }

    public static String getLocationQuery(TransactionEventRaw transactionEventRaw) {
        return String.format(
                "select * from locations where account_id='%s' and hash='%s';",
                transactionEventRaw.getTransaction().getAccountId(),
                transactionEventRaw.getLocation().getHash()
        );
    }

}
