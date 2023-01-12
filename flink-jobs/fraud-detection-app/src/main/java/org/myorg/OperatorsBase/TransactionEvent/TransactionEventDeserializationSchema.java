package org.myorg.OperatorsBase.TransactionEvent;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.myorg.Events.TransactionEvent.TransactionEventRaw;

import java.io.IOException;


public class TransactionEventDeserializationSchema
        implements KafkaRecordDeserializationSchema<TransactionEventRaw> {

    private transient Deserializer<String> deserializer;

    /**
     * Deserializes the byte message.
     *
     * <p>Can output multiple records through the {@link Collector}. Note that number and size of
     * the produced records should be relatively small. Depending on the source implementation
     * records can be buffered in memory or collecting records might delay emitting checkpoint
     * barrier.
     *
     * @param record The ConsumerRecord to deserialize.
     * @param out    The collector to put the resulting messages.
     */
    @Override
    public void deserialize(
            ConsumerRecord<byte[], byte[]> record, Collector<TransactionEventRaw> out
    ) throws IOException {
        if (deserializer == null) {
            deserializer = new StringDeserializer();
        }

        String value = deserializer.deserialize(record.topic(), record.value());

        try {
            Gson gson = new GsonBuilder()
                    .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                    .create();
            TransactionEventRaw transactionEventRaw = gson.fromJson(value, TransactionEventRaw.class);
            transactionEventRaw = new TransactionEventRaw(transactionEventRaw);
            out.collect(transactionEventRaw);
        } catch (IllegalStateException | JsonSyntaxException exception) {
            System.out.println(exception.toString());
        }
    }

    /**
     * Gets the data type (as a {@link TypeInformation}) produced by this function or input format.
     *
     * @return The data type produced by this function or input format.
     */
    @Override
    public TypeInformation<TransactionEventRaw> getProducedType() {
        return TypeInformation.of(TransactionEventRaw.class);
    }
}
