package org.myorg.OperatorsBase;

import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.myorg.Cache.ModelSessionCache;
import org.myorg.EnrichmentMethods.ModelRaw;
import org.myorg.Events.LogEvent.LogEvent;
import org.myorg.Events.LogEvent.LogEventResult;
import org.myorg.Events.LogEvent.ModelSession;
import org.myorg.OperatorsBase.LogEvent.*;

public class LogAnalysis {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(600_000);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        ParameterTool parameters = ParameterTool.fromArgs(args);
        ExternalConfiguration config = new ExternalConfiguration(parameters);

        KafkaSource<LogEvent> source = KafkaSource.<LogEvent>builder()
                .setBootstrapServers(String.format("%s:%s", config.getKafkaHost(), config.getKafkaPort()))
                .setDeserializer(new LogEventDeserializationSchema())
                .setTopics(config.getKafkaInputTopic())
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        DataStream<LogEvent> input = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Event-Source"
        );

//      add window start timestamp
        input = input.flatMap(new RichFlatMapFunction<LogEvent, LogEvent>() {
            @Override
            public void flatMap(LogEvent value, Collector<LogEvent> out) throws Exception {
                value.setWindowStartTimestamp(System.currentTimeMillis());
                out.collect(value);
            }
        }).name("Add-Window-Start-Time");

        int cacheMaxSize = config.getCacheMaxSize();
        int cacheFlushIntervalMs = config.getCacheFlushIntervalMs();
        String modelURL = config.getModelURL();

        DataStream<LogEventResult> mainDataStream = input
                .keyBy(LogEvent::getKey)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .process(new ProcessWindowFunction<LogEvent, LogEventResult, String, TimeWindow>() {

                    public transient Counter totalEvents, cacheHitMetric;
                    public transient Histogram cacheHitHistogram, latencyHistogram;
                    public transient Histogram initSessionHistogram, predictHistogram;

                    private ValueState<ModelRaw> modelState;
                    private ModelSessionCache cache;
                    private long nextCacheFreeTime;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        cache = new ModelSessionCache(cacheMaxSize, getRuntimeContext().getIndexOfThisSubtask());

                        ValueStateDescriptor<ModelRaw>  modelStateDescriptor = new ValueStateDescriptor<>(
                                "modelState", ModelRaw.class
                        );
                        modelState = getRuntimeContext().getState(modelStateDescriptor);

                        totalEvents = getRuntimeContext().getMetricGroup().counter("totalEventsModel");
                        cacheHitMetric = getRuntimeContext().getMetricGroup().counter("cacheHitMetricModel");

                        com.codahale.metrics.Histogram dropwizardHistogram =
                                new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));
                        cacheHitHistogram = getRuntimeContext()
                                .getMetricGroup()
                                .histogram("cacheHitHistogramModel", new DropwizardHistogramWrapper(dropwizardHistogram));

                        com.codahale.metrics.Histogram dropwizardHistogram2 =
                                new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));
                        latencyHistogram = getRuntimeContext()
                                .getMetricGroup()
                                .histogram("latencyHistogramModel", new DropwizardHistogramWrapper(dropwizardHistogram2));

                        com.codahale.metrics.Histogram dropwizardHistogram3 =
                                new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));
                        initSessionHistogram = getRuntimeContext()
                                .getMetricGroup()
                                .histogram("initSessionHistogramModel", new DropwizardHistogramWrapper(dropwizardHistogram3));

                        com.codahale.metrics.Histogram dropwizardHistogram4 =
                                new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));
                        predictHistogram = getRuntimeContext()
                                .getMetricGroup()
                                .histogram("predictHistogramModel", new DropwizardHistogramWrapper(dropwizardHistogram4));

                        nextCacheFreeTime = System.currentTimeMillis() + cacheFlushIntervalMs;
                    }

                    @Override
                    public void process(
                            String s, 
                            Context context, 
                            Iterable<LogEvent> elements, 
                            Collector<LogEventResult> out
                    ) throws Exception {
                        if (modelState.value() == null) {
                            ModelRaw modelRaw = new ModelRaw();
                            modelRaw.load(modelURL);
                            modelState.update(modelRaw);
                        }

                        long ts = System.currentTimeMillis();
                        for (LogEvent logEvent : elements) {
                            logEvent.setWindowEndTimestamp(ts);
                        }

                        totalEvents.inc();

                        ModelSession modelSession;
                        if (cache.containsKey(s)) {
                            cacheHitMetric.inc();
                            modelSession = cache.get(s);
                        } else {
                            System.out.println("Create new Session!");
                            modelSession = new ModelSession();
                            long initTime = modelSession.loadSession(modelState.value());
                            initSessionHistogram.update(initTime);
                            cache.add(s, modelSession);
                        }
                        float[] result = null;
                        if (modelSession != null) {
                            long start = System.currentTimeMillis();
                            result = modelSession.predict();
                            long predictionTime = System.currentTimeMillis() - start;
                            predictHistogram.update(predictionTime);
                        } else {
                            System.out.printf("Model Session is Null. KEY: %s\n", s);
                        }

                        ts = System.currentTimeMillis();
                        for (LogEvent logEvent : elements) {
                            long totalDuration = ts - logEvent.getTimestamp();
                            long windowDuration = logEvent.getWindowEndTimestamp() - logEvent.getWindowStartTimestamp();
                            long latency = totalDuration - windowDuration;
                            latencyHistogram.update(latency);
//                            System.out.printf("LATENCY: %s\n", latency);
                        }

                        int cacheHitRate = (int) (((float) cacheHitMetric.getCount() / (float) totalEvents.getCount())  * 100);
                        cacheHitHistogram.update(cacheHitRate);

                        if (System.currentTimeMillis() > nextCacheFreeTime) {
                            nextCacheFreeTime = System.currentTimeMillis() + cacheFlushIntervalMs;
                            System.out.printf("======CLEAR CACHE======: %s; \n", getRuntimeContext().getIndexOfThisSubtask());
                            cache.clear();
                        }
                        out.collect(new LogEventResult(s, result, context.window().getStart(), context.window().getEnd()));
                    }
                });

        KafkaSink<LogEventResult> sink = KafkaSink.<LogEventResult>builder()
                .setBootstrapServers(String.format("%s:%s", config.getKafkaHost(), config.getKafkaPort()))
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(config.getKafkaOutputTopic())
                                .setKafkaValueSerializer(LogEventResultSerializationSchema.class)
                                .build()
                )
                .build();
        mainDataStream.sinkTo(sink);

        env.execute("Stream-Job-Log-Analysis");
    }
}
