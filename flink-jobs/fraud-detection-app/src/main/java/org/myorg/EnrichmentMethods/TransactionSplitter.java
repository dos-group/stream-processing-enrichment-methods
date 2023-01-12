package org.myorg.EnrichmentMethods;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.myorg.Events.TransactionEvent.*;

public class TransactionSplitter  {
    public static DataStream<EventBase> getTransactionStream(DataStream<TransactionEventRaw> input) {
        return input.flatMap(new FlatMapFunction<TransactionEventRaw, EventBase>() {
            /**
             * The core method of the FlatMapFunction. Takes an element from the input data set and
             * transforms it into zero, one, or more elements.
             *
             * @param value The input value.
             * @param out   The collector for returning result values.
             * @throws Exception This method may throw exceptions. Throwing an exception will cause the
             *                   operation to fail and may trigger recovery.
             */
            @Override
            public void flatMap(TransactionEventRaw value, Collector<EventBase> out) throws Exception {
                out.collect(value.getTransaction());
            }
        }).name("Raw-Event-To-Transaction-Events");
    }

    public static DataStream<EventBase> getDeviceStream(DataStream<TransactionEventRaw> input) {
        return input.flatMap(new FlatMapFunction<TransactionEventRaw, EventBase>() {
            /**
             * The core method of the FlatMapFunction. Takes an element from the input data set and
             * transforms it into zero, one, or more elements.
             *
             * @param value The input value.
             * @param out   The collector for returning result values.
             * @throws Exception This method may throw exceptions. Throwing an exception will cause the
             *                   operation to fail and may trigger recovery.
             */
            @Override
            public void flatMap(TransactionEventRaw value, Collector<EventBase> out) throws Exception {
                out.collect(value.getDevice());
            }
        }).name("Raw-Event-To-Device-Events");
    }

    public static DataStream<EventBase> getLocationStream(DataStream<TransactionEventRaw> input) {
        return input.flatMap(new FlatMapFunction<TransactionEventRaw, EventBase>() {
            /**
             * The core method of the FlatMapFunction. Takes an element from the input data set and
             * transforms it into zero, one, or more elements.
             *
             * @param value The input value.
             * @param out   The collector for returning result values.
             * @throws Exception This method may throw exceptions. Throwing an exception will cause the
             *                   operation to fail and may trigger recovery.
             */
            @Override
            public void flatMap(TransactionEventRaw value, Collector<EventBase> out) throws Exception {
                out.collect(value.getLocation());
            }
        }).name("Raw-Event-To-Location-Events");
    }

    public static DataStream<TransactionEventRaw> getJoinedTransaction(
            DataStream<EventBase> transactions,
            DataStream<EventBase> devices,
            DataStream<EventBase> locations
    ) {
        DataStream<TransactionEventRaw> transactionDeviceJoinStream = transactions.join(devices)
                .where((KeySelector<EventBase, String>) value -> {
                    return value.getTransactionId();
                })
                .equalTo((KeySelector<EventBase, String>) value -> {
                    return value.getTransactionId();
                })
//                .window(joinWindow)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(2)))
                .apply(new JoinFunction<EventBase, EventBase, TransactionEventRaw>() {
                    /**
                     * The join method, called once per joined pair of elements.
                     *
                     * @param first  The element from first input.
                     * @param second The element from second input.
                     * @return The resulting element.
                     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
                     *                   operation to fail and may trigger recovery.
                     */
                    @Override
                    public TransactionEventRaw join(EventBase first, EventBase second) throws Exception {
                        TransactionEventRaw res = new TransactionEventRaw();
                        res.setTransaction((TransactionEvent) first);
                        res.setDevice((DeviceEvent) second);
                        return res;
                    }
                });

//        transactionDeviceJoinStream.print();

        return transactionDeviceJoinStream.join(locations)
                .where(new KeySelector<TransactionEventRaw, String>() {
                    @Override
                    public String getKey(TransactionEventRaw value) throws Exception {
                        return value.getTransaction().getTransactionId();
                    }
                })
                .equalTo(new KeySelector<EventBase, String>() {
                    @Override
                    public String getKey(EventBase value) throws Exception {
                        return value.getTransactionId();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.milliseconds(4000)))
                .apply(new JoinFunction<TransactionEventRaw, EventBase, TransactionEventRaw>() {
                    @Override
                    public TransactionEventRaw join(TransactionEventRaw first, EventBase second) throws Exception {
                        first.setLocation((LocationEvent) second);
                        return first;
                    }
                });
    }
}
