package com.dch.tutorial.beam;

import com.dch.tutorial.beam.transform.GenerateFruit;
import com.dch.tutorial.beam.util.LogUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

import java.util.Arrays;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class WordCountStreamingApplication {

    public static void main(String... args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply(GenerateFruit.randomly())
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
                .apply(LogUtil.ofElements());

        pipeline.run();
    }
}
