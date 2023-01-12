package org.myorg.OperatorsBase.TransactionEvent;

import com.google.gson.*;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.myorg.Events.TransactionEvent.TransactionResult;

import java.util.Map;


public class TransactionEventSerializationSchema implements Serializer<TransactionResult> {

    /**
     * Convert {@code data} into a byte array.
     *
     * @param topic topic associated with data
     * @param data  typed data
     * @return serialized bytes
     */
    @Override
    public byte[] serialize(String topic, TransactionResult data) {
        Gson gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .enableComplexMapKeySerialization()
                .create();

        String dataJson = gson.toJson(data);
        JsonElement jsonElement = JsonParser.parseString(dataJson);
        JsonObject jsonObject = jsonElement.getAsJsonObject();

        /*
        list of suspicious transactions needs to be serialized
        separately because otherwise enums are not serialized
        */
        String suspiciousTransactionsJson = gson.toJson(data.getSuspiciousTransactions());
        JsonElement suspiciousTransactionsJsonElement = JsonParser.parseString(suspiciousTransactionsJson);
        jsonObject.add("suspicious_transactions", suspiciousTransactionsJsonElement);

        return gson.toJson(jsonObject).getBytes();
    }

    /**
     * Convert {@code data} into a byte array.
     *
     * @param topic   topic associated with data
     * @param headers headers associated with the record
     * @param data    typed data
     * @return serialized bytes
     */
    @Override
    public byte[] serialize(String topic, Headers headers, TransactionResult data) {
        return null;
    }

}
