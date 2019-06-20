package com.dch.tutorial.beam;

import com.dch.tutorial.beam.util.LogUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;

import java.util.Arrays;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;

/**
 * Pipeline that counts the number of words.
 * <p>
 * Please output the count of each word in the following format:
 * <pre>
 *   word:count
 *   ball:5
 *   book:3
 * </pre>
 *
 * @author david.christianto
 */
public class WordCountApplication {

    public static void main(String... args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Create sentences",
                        Create.of(Arrays.asList("apple orange grape banana apple banana", "banana orange papaya")))
                .apply("Flat Map to words",
                        FlatMapElements.into(strings()).via(sentence -> Arrays.asList(sentence.split(" "))))
                .apply("Count per word", Count.perElement())
                .apply("Map to KV",
                        MapElements.into(strings()).via(kv -> String.format("%s:%d", kv.getKey(), kv.getValue())))
                .apply(LogUtil.ofElements());

        pipeline.run();
    }
}
