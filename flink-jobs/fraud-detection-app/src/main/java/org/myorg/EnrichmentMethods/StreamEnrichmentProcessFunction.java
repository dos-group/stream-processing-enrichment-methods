package org.myorg.EnrichmentMethods;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.myorg.Events.TransactionEvent.EventBase;
import org.myorg.Events.TransactionEvent.SuspiciousType;
import org.myorg.Events.TransactionEvent.TransactionEventRaw;

import java.util.Map;

public class StreamEnrichmentProcessFunction
        extends CoProcessFunction<TransactionEventRaw, EventBase, TransactionEventRaw> {

    private final SuspiciousType suspiciousType;
    private MapState<Long, EventBase> mapStateDbEventBase;

    public StreamEnrichmentProcessFunction(SuspiciousType suspiciousType) {
        this.suspiciousType = suspiciousType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<Long, EventBase> mapStateDescriptor = new MapStateDescriptor<Long, EventBase>(
                "dbData",
                Long.class,
                EventBase.class
        );
        mapStateDbEventBase = getRuntimeContext().getMapState(mapStateDescriptor);
    }

    /**
     * This method is called for each element in the first of the connected streams.
     *
     * <p>This function can output zero or more elements using the {@link Collector} parameter and
     * also update internal state or set timers using the {@link Context} parameter.
     *
     * @param value The stream element
     * @param ctx   A {@link Context} that allows querying the timestamp of the element, querying the
     *              {@link TimeDomain} of the firing timer and getting a {@link TimerService} for registering
     *              timers and querying the time. The context is only valid during the invocation of this
     *              method, do not store it.
     * @param out   The collector to emit resulting elements to
     * @throws Exception The function may throw exceptions which cause the streaming program to fail
     *                   and go into recovery.
     */
    @Override
    public void processElement1(TransactionEventRaw value, Context ctx, Collector<TransactionEventRaw> out) throws Exception {
        long currMaxTs = -1L;
        for (Map.Entry<Long, EventBase> entry : mapStateDbEventBase.entries()) {
            if (value.getTransaction().getTimestamp() >= entry.getKey() && entry.getKey() > currMaxTs)
                currMaxTs = entry.getKey();
        }
        if (!mapStateDbEventBase.contains(currMaxTs)) {
            value = EnrichmentHelper.enrichTransaction(value, suspiciousType);
        }
        out.collect(value);
    }

    /**
     * This method is called for each element in the second of the connected streams.
     *
     * <p>This function can output zero or more elements using the {@link Collector} parameter and
     * also update internal state or set timers using the {@link Context} parameter.
     *
     * @param value The stream element
     * @param ctx   A {@link Context} that allows querying the timestamp of the element, querying the
     *              {@link TimeDomain} of the firing timer and getting a {@link TimerService} for registering
     *              timers and querying the time. The context is only valid during the invocation of this
     *              method, do not store it.
     * @param out   The collector to emit resulting elements to
     * @throws Exception The function may throw exceptions which cause the streaming program to fail
     *                   and go into recovery.
     */
    @Override
    public void processElement2(EventBase value, Context ctx, Collector<TransactionEventRaw> out) throws Exception {
        mapStateDbEventBase.put(value.getTimestamp(), value);
    }
}
