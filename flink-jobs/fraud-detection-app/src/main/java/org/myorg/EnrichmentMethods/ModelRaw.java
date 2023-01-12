package org.myorg.EnrichmentMethods;

import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;

public class ModelRaw implements Serializable {
    private byte[] model;

    public ModelRaw() {}

    public byte[] getModel() {
        return model;
    }

    public void setModel(byte[] model) {
        this.model = model;
    }

    public long load(String modelURL) throws Exception {
        long start = System.currentTimeMillis();

        if (model == null)
            model = fetchRemoteFile(modelURL);

        long fetchTime = System.currentTimeMillis() - start;
//        System.out.printf("Fetch Time: %s\n", fetchTime);
        return fetchTime;
    }

    private byte[] fetchRemoteFile(String modelURL) throws Exception {
        System.out.println("FETCH MODEL!");
        URL url = new URL(modelURL);
        InputStream inputStream = url.openStream();
        byte[] bytes = IOUtils.toByteArray(inputStream);

        if (inputStream != null) inputStream.close();

        return bytes;
    }

}
