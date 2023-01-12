package org.myorg.Events.LogEvent;

import ai.onnxruntime.*;
import org.myorg.EnrichmentMethods.ModelRaw;

import java.util.HashMap;
import java.util.Map;

public class ModelSession {

    public OrtSession ortSession;
    public OrtEnvironment ortEnvironment;

    public ModelSession() {}

    public long loadSession(ModelRaw modelRaw) throws OrtException {
        long start = System.currentTimeMillis();
        ortEnvironment = OrtEnvironment.getEnvironment();

        OrtSession.SessionOptions sessionOptions = new OrtSession.SessionOptions();
        ortSession = ortEnvironment.createSession(modelRaw.getModel(), sessionOptions);
        long initSessionTime = System.currentTimeMillis() - start;
//        System.out.printf("INIT SESSION TIME: %s\n", initSessionTime);
        return initSessionTime;
    }

    public float[] predict() throws OrtException {
        float[][][][] input = new float[1][3][224][224];
        OnnxTensor inputOnnxTensor = OnnxTensor.createTensor(ortEnvironment, input);
        Map<String, OnnxTensor> onnxInputs = Map.of("data", inputOnnxTensor);

        OrtSession.Result result = ortSession.run(onnxInputs);
        float[][] onnxOutput = (float[][]) result.get(0).getValue();
        return onnxOutput[0];
    }

}
