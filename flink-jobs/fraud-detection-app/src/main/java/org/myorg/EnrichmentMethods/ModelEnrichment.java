package org.myorg.EnrichmentMethods;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.myorg.Events.TransactionEvent.TransactionEventRaw;


public class ModelEnrichment extends KeyedProcessFunction<String, TransactionEventRaw, String> {

    private ValueState<OnnxModel> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", OnnxModel.class));
    }

    /**
     * Process one element from the input stream.
     *
     * <p>This function can output zero or more elements using the {@link Collector} parameter and
     * also update internal state or set timers using the {@link Context} parameter.
     *
     * @param value The input value.
     * @param ctx   A {@link Context} that allows querying the timestamp of the element and getting a
     *              {@link TimerService} for registering timers and querying the time. The context is only
     *              valid during the invocation of this method, do not store it.
     * @param out   The collector for returning result values.
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *                   operation to fail and may trigger recovery.
     */
    @Override
    public void processElement(TransactionEventRaw value, Context ctx, Collector<String> out) throws Exception {
        if (state.value() == null) {
            System.out.println("LOAD MODEL");
            OnnxModel model = new OnnxModel();
            model.loadOnnx("https://storage.googleapis.com/streaming-jobs-1/jars/resnet18-v1-7.onnx");
//            model.loadOnnx("https://storage.googleapis.com/streaming-jobs-1/jars/resnet152-v2-7.onnx");
            state.update(model);
        }

//        System.out.println("MODEL PRINT");
        OnnxModel tmp = state.value();
        tmp.predictOnnx("https://storage.googleapis.com/streaming-jobs-1/jars/resnet18-v1-7.onnx");
//        state.update(tmp);
    }

}
