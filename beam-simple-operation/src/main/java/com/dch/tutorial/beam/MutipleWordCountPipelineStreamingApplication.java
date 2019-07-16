package com.dch.tutorial.beam;

import com.dch.tutorial.beam.option.BranchPipelineOptions;
import com.dch.tutorial.beam.transform.GenerateFruit;
import com.dch.tutorial.beam.util.LogUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.util.Arrays;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;

/**
 * Pipeline that stream the input then counts the number of words.
 * <p>
 * Please output the count of each word in the following format:
 * <pre>
 *   word:count
 *   ball:5
 *   book:3
 * </pre>
 * In this example with create multiple pipeline in single job (branching pipeline).
 *
 * @author david.christianto
 */
public class MutipleWordCountPipelineStreamingApplication {

    public static void main(String... args) {
        BranchPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(BranchPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> fruits = pipeline.apply(GenerateFruit.randomly());

        String[] prefixes = options.getPrefixes().split(",");
        for (String prefix : prefixes) {
            createPipeline(fruits, prefix);
        }

        pipeline.run();
    }

    /**
     * Method to create WordCountStreaming pipeline with specified prefix.
     *
     * @param fruits {@link PCollection}
     * @param prefix Prefix to log each elements.
     * @return {@link Pipeline} WordCountStreaming.
     */
    private static PCollection<String> createPipeline(PCollection<String> fruits, String prefix) {
        return fruits
                .apply("Flat Map to words",
                        FlatMapElements.into(strings()).via(sentence -> Arrays.asList(sentence.split(" "))))
                .apply("Window",
                        Window.<String>into(FixedWindows.of(Duration.standardDays(1)))
                                .triggering(AfterWatermark.pastEndOfWindow()
                                        .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                                                .alignedTo(Duration.standardSeconds(5))))
                                .withAllowedLateness(Duration.ZERO)
                                .discardingFiredPanes())
                .apply("Count per word", Count.perElement())
                .apply("Map to KV",
                        MapElements.into(strings()).via(kv -> String.format("%s:%d", kv.getKey(), kv.getValue())))
                .apply(LogUtil.ofElements(prefix));
    }
}
