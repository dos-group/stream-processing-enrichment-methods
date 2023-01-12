package org.myorg.OperatorsBase.LogEvent;

import com.google.gson.*;
import org.apache.kafka.common.serialization.Serializer;
import org.myorg.Events.LogEvent.LogEventResult;


public class LogEventSerializationSchema implements Serializer<LogEventResult> {

    /**
     * Convert {@code data} into a byte array.
     *
     * @param topic topic associated with data
     * @param data  typed data
     * @return serialized bytes
     */
    @Override
    public byte[] serialize(String topic, LogEventResult data) {
        Gson gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .enableComplexMapKeySerialization()
                .create();

        return gson.toJson(data).getBytes();
    }

}
