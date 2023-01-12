package org.myorg.EnrichmentMethods;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OnnxValue;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtSession;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class OnnxModel implements Serializable {
//    public OrtSession session;
//    public OrtEnvironment env;
    public byte[] model;

    public OnnxModel() {}

    public void loadOnnx(String modelPath) throws Exception {
        long start = System.currentTimeMillis();

        if (model == null)
            model = fetchRemoteFile(modelPath);
        else
            System.out.println("\nMODEL FOUND IN STATE!\n");

        long startSession = System.currentTimeMillis();

//        if (env == null)
//            env = OrtEnvironment.getEnvironment();
//        else
//            System.out.println("\nENV NOT NULL!\n");
//
//        if (session == null) {
//            OrtSession.SessionOptions sessionOptions = new OrtSession.SessionOptions();
//            session = env.createSession(model, sessionOptions);
//            env.createSession(model, sessionOptions);
//        } else {
//            System.out.println("\nSESSION NOT NULL!\n");
//        }

//        long end = System.currentTimeMillis();
        long fetchTime = startSession - start;
//        long initSession = end - startSession;

        System.out.printf("\nFetch Time: %s\n", fetchTime);
//        System.out.printf("Fetch Time: %s; Init Session Time: %s", fetchTime, initSession);
    }

    public void predictOnnx(String modelPath) throws Exception {
        loadOnnx(modelPath);

//        long start = System.currentTimeMillis();
//
//        float[][][][] input = new float[1][3][224][224];
//        OnnxTensor inputONNXTensor = OnnxTensor.createTensor(env, input);
//        Map<String, OnnxTensor> onnxInputs = new HashMap<>();
//        onnxInputs.put("data", inputONNXTensor);

//        System.out.printf("\nSession: %s\n", session);
//        System.out.printf("Tensor: %s\n", inputONNXTensor);

        // Run the inputs through the ONNX model
//        OnnxValue onnxValueResults = session.run(onnxInputs).get(0);
//        float[][] onnxOutput = (float[][]) onnxValueResults.getValue();

//        System.out.printf("\nPREDICTION TIME: %s\n", (System.currentTimeMillis() - start));

//        ArrayList<Float> result = new ArrayList<>();
//        for (float f : onnxOutput[0])
//            result.add(f);
//        return result;
    }

    private byte[] fetchRemoteFile(String location) throws Exception {
        System.out.println("FETCH MODEL!");
        URL url = new URL(location);
        InputStream inputStream = url.openStream();
        byte[] bytes = IOUtils.toByteArray(inputStream);

        if (inputStream != null) inputStream.close();

//        Byte[] res = new Byte[bytes.length];
//        int i = 0;
//        for(byte b: bytes)
//            res[i++] = b;

        return bytes;
    }

}
