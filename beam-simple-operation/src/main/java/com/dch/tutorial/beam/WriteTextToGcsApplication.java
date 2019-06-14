package com.dch.tutorial.beam;

import com.google.auth.oauth2.ServiceAccountCredentials;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.List;

/**
 * Simple write text to GCS application using {@link DataflowRunner}.
 *
 * @author David.Christianto
 */
public class WriteTextToGcsApplication {

    private static final List<String> SCOPES = Arrays.asList(
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/devstorage.full_control",
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/datastore",
            "https://www.googleapis.com/auth/pubsub");

    public static void main(String... args) throws Exception {
        try (FileInputStream inputStream = new FileInputStream("beam-simple-operation/src/main/resources/key.json")) {
            DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
            options.setGcpCredential(ServiceAccountCredentials.fromStream(inputStream).createScoped(SCOPES));
            options.setJobName("write-text-to-gcs-job");
            options.setRunner(DataflowRunner.class);

            Pipeline pipeline = Pipeline.create(options);
            pipeline.apply("Collect list of text", Create.of(Arrays.asList("data1", "data2", "data3")))
                    .apply("Write text to GCS",
                            TextIO.write().to("gs://dataeng-poc-storage/davidch/result/test.txt").withNumShards(1));
            pipeline.run().waitUntilFinish();
        }
    }
}
