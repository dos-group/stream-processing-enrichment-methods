package org.myorg.EnrichmentMethods;

import ai.djl.MalformedModelException;
import ai.djl.inference.Predictor;
import ai.djl.modality.Classifications;
import ai.djl.modality.cv.Image;
import ai.djl.modality.cv.ImageFactory;
import ai.djl.modality.cv.transform.CenterCrop;
import ai.djl.modality.cv.transform.Normalize;
import ai.djl.modality.cv.transform.Resize;
import ai.djl.modality.cv.transform.ToTensor;
import ai.djl.modality.cv.translator.ImageClassificationTranslator;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ModelZoo;
import ai.djl.translate.*;

import java.io.IOException;
import java.io.Serializable;


public class TorchModel implements Serializable {
    public Predictor<Image, Classifications> predictor = null;
    private Image predictImage = null;

    public void predictTorch() throws TranslateException {
        Classifications classifications = predictor.predict(predictImage);
        System.out.printf("RESULT: %s", classifications);
    }

    public void loadTorch() throws MalformedModelException, ModelNotFoundException, IOException, TranslateException {
        System.out.printf("MODELS LIST: %s", ModelZoo.listModels());
//        DownloadUtils.download(
//                "https://djl-ai.s3.amazonaws.com/mlrepo/model/cv/image_classification/ai/djl/pytorch/resnet/0.0.1/traced_resnet18.pt.gz",
//                "src/main/resources/resnet18/resnet18.pt",
//                new ProgressBar()
//        );
//        DownloadUtils.download(
//                "https://djl-ai.s3.amazonaws.com/mlrepo/model/cv/image_classification/ai/djl/pytorch/synset.txt",
//                "src/main/resources/resnet18/synset.txt",
//                new ProgressBar()
//        );
        long start = System.currentTimeMillis();
        Translator<Image, Classifications> translator = ImageClassificationTranslator.builder()
                .addTransform(new Resize(256))
                .addTransform(new CenterCrop(224, 224))
                .addTransform(new ToTensor())
                .addTransform(new Normalize(
                        new float[] {0.485f, 0.456f, 0.406f},
                        new float[] {0.229f, 0.224f, 0.225f}))
                .optApplySoftmax(true)
                .optSynsetUrl("https://djl-ai.s3.amazonaws.com/mlrepo/model/cv/image_classification/ai/djl/pytorch/synset.txt")
                .build();

//        URL modelPath = Thread.currentThread().getContextClassLoader().getResource("resnet18");
//        System.out.printf("\nPATH: %s\n", modelPath.getPath());
//        System.out.println("TEST OUTPUT");

        Criteria<Image, Classifications> criteria = Criteria.builder()
                .setTypes(Image.class, Classifications.class)
                .optTranslator(translator)
                .optModelUrls("https://storage.googleapis.com/streaming-jobs-1/jars/resnet18.zip")
//                .optOption("mapLocation", "true") // this model requires mapLocation for GPU
//                .optProgress(new ProgressBar())
                .build();

        predictImage = ImageFactory.getInstance().fromUrl("https://raw.githubusercontent.com/pytorch/hub/master/images/dog.jpg");
        predictImage.getWrappedImage();

        predictor = criteria.loadModel().newPredictor();
        System.out.printf("\nLOAD MODEL TIME %s\n", (System.currentTimeMillis() - start));
    }

}
