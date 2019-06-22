package com.dch.tutorial.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;

import java.util.Arrays;

/**
 * Simple write text to GCS application.
 *
 * @author David.Christianto
 */
public class WriteTextToGcsApplication {

    public static void main(String... args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
        options.setJobName("write-text-to-gcs-job");

        // If you are using DataflowRunner, you can set credential with Google Service Accounts.
        // options.setGcpCredential(ServiceAccountCredentials.fromStream(inputStream).createScoped(SCOPES));

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("Collect list of text", Create.of(Arrays.asList("data1", "data2", "data3")))
                .apply("Write text to GCS",
                        TextIO.write().to("gs://dataeng-poc-storage/davidch/result/test.txt").withNumShards(1));
        pipeline.run().waitUntilFinish();
    }
}
