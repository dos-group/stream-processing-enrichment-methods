package org.myorg.OperatorsBase.LogEvent;

import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.myorg.EnrichmentMethods.ModelRaw;
import org.myorg.Events.LogEvent.CacheInfoEvent;
import org.myorg.Events.LogEvent.LogEvent;

public class LogModelStateProcessFunction extends CoProcessFunction<LogEvent, CacheInfoEvent, LogEvent> {
    private ValueState<Integer> cacheInfoState;
    private ValueState<ModelRaw> modelState;

    private final String modelURL;

    public transient Histogram fetchModelHistogram;

    public LogModelStateProcessFunction(String modelURL) {
        this.modelURL = modelURL;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Integer> cacheInfoStateDescriptor = new ValueStateDescriptor<>(
                "cacheInfoState", Integer.class
        );
        cacheInfoState = getRuntimeContext().getState(cacheInfoStateDescriptor);

        ValueStateDescriptor<ModelRaw>  modelStateDescriptor = new ValueStateDescriptor<>(
                "modelState", ModelRaw.class
        );
        modelState = getRuntimeContext().getState(modelStateDescriptor);

        com.codahale.metrics.Histogram dropwizardHistogram =
                new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));
        fetchModelHistogram = getRuntimeContext()
                .getMetricGroup()
                .histogram("fetchModelHistogram", new DropwizardHistogramWrapper(dropwizardHistogram));
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
    public void processElement1(LogEvent value, Context ctx, Collector<LogEvent> out) throws Exception {
        if (cacheInfoState.value() == null || cacheInfoState.value() == 0) {
            if (modelState.value() == null) {
                System.out.println("FETCH MODEL!");
                ModelRaw modelRaw = new ModelRaw();
                long fetchTime = modelRaw.load(modelURL);
                value.setModelRaw(modelRaw);
                modelState.update(modelRaw);

                fetchModelHistogram.update(fetchTime);
            } else {
                System.out.println("GET MODEL FROM STATE");
                value.setModelRaw(modelState.value());
            }
        }
        cacheInfoState.update(1);
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
    public void processElement2(CacheInfoEvent value, Context ctx, Collector<LogEvent> out) throws Exception {
//        System.out.printf("CACHE INFO %s: %s\n", value.getKey(), value.getIsCached());
        cacheInfoState.update(value.getIsCached());
    }

}
