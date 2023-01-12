package org.myorg.EnrichmentMethods;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import org.myorg.Events.TransactionEvent.TransactionResult;

public class RedisExampleMapper implements RedisMapper<Tuple2<String, String>> {

    public static DataStream<Tuple2<String, String>> getSinkStream(DataStream<TransactionResult> input) {
        return input.flatMap(new FlatMapFunction<TransactionResult, Tuple2<String, String>>() {
            @Override
            public void flatMap(TransactionResult value, Collector<Tuple2<String, String>> out) throws Exception {
                Tuple2 res = new Tuple2(value.getAccountId(), Integer.toString(value.getTransactionAmount()));
                out.collect(res);
            }
        });
    }

    /**
     * Returns descriptor which defines data type.
     *
     * @return data type descriptor
     */
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME");
    }

    /**
     * Extracts key from data.
     *
     * @param data source data
     * @return key
     */
    @Override
    public String getKeyFromData(Tuple2<String, String> data) {
        return data.f0;
    }

    /**
     * Extracts value from data.
     *
     * @param data source data
     * @return value
     */
    @Override
    public String getValueFromData(Tuple2<String, String> data) {
        return data.f1;
    }
}
