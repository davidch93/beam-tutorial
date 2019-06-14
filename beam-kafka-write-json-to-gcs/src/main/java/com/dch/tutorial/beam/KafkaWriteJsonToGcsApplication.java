package com.dch.tutorial.beam;

import com.dch.tutorial.beam.option.WriteJsonOptions;
import com.dch.tutorial.beam.transform.FilterTombStoneRecord;
import com.dch.tutorial.beam.util.TimeUtil;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Apache Beam streaming application from Kafka, transform the record, and store it to GCS with JSON format.
 *
 * @author David.Christianto
 */
public class KafkaWriteJsonToGcsApplication {

    public static void main(String... args) {
        WriteJsonOptions options = PipelineOptionsFactory.fromArgs(args).as(WriteJsonOptions.class);
        options.setJobName("kafka-write-json-to-gcs-job");

        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply(String.format("Read Kafka Events from `%s`", options.getTopic().get()),
                        KafkaIO.<String, String>read()
                                .withBootstrapServers(options.getBootstrapServers().get())
                                .withTopic(options.getTopic().get())
                                .withKeyDeserializer(StringDeserializer.class)
                                .withValueDeserializer(StringDeserializer.class)
                                .updateConsumerProperties(ImmutableMap.of("group.id", options.getGroupId().get(),
                                        "auto.offset.reset", options.getAutoOffsetReset().get()))
                                .withReadCommitted()
                                .commitOffsetsInFinalize()
                                .withoutMetadata())
                .apply("Collect Value from Kafka", Values.create())
                .apply(options.getWindowDuration().get() + " Window",
                        Window.into(FixedWindows.of(TimeUtil.parseDuration(options.getWindowDuration().get()))))
                .apply("Filter is not Tombstone Flag Record",
                        Filter.by(new FilterTombStoneRecord()))
//                    .apply("Transform Record", null)
                .apply("Write File(s) to GCS",
                        TextIO.write()
                                .withWindowedWrites()
                                .withNumShards(options.getNumShards().get())
                                .withSuffix(".json")
                                .to(options.getOutputDirectory()));
        pipeline.run();
    }
}
