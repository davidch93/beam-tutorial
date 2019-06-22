package com.dch.tutorial.beam;

import com.dch.tutorial.beam.option.WriteJsonOptions;
import com.dch.tutorial.beam.policy.DatePartitionedNamePolicy;
import com.dch.tutorial.beam.policy.EventTimePolicy;
import com.dch.tutorial.beam.transform.KafkaRecordTransformer;
import com.dch.tutorial.beam.transform.TombStoneRecordFilter;
import com.dch.tutorial.beam.util.TimeUtil;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
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
        System.out.println(String.join(", ", args));

        WriteJsonOptions options = PipelineOptionsFactory.fromArgs(args).as(WriteJsonOptions.class);
        options.setJobName("kafka-write-json-to-gcs-job");
        ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(options.getOutputDirectory().get());

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
                                .withTimestampPolicyFactory((tp, previousWatermark) ->
                                        EventTimePolicy.withEventTime(previousWatermark))
                                .withReadCommitted()
                                .commitOffsetsInFinalize()
                                .withoutMetadata())
                .apply("Collect Value from Kafka", Values.create())
                .apply(options.getWindowDuration().get() + " Window",
                        Window.<String>into(FixedWindows.of(TimeUtil.parseDuration(options.getWindowDuration().get())))
                                .withAllowedLateness(TimeUtil.parseDuration(options.getLateDuration().get()))
                                .discardingFiredPanes())
                .apply("Filter is not Tombstone Flag Record", Filter.by(TombStoneRecordFilter.filter()))
                .apply("Transform Record", KafkaRecordTransformer.transform())
                .apply("Write File(s) to GCS",
                        TextIO.write()
                                .to(DatePartitionedNamePolicy.withDatePartitioned(resource))
                                .withTempDirectory(resource.getCurrentDirectory())
                                .withWindowedWrites()
                                .withNumShards(options.getNumShards().get()));
        pipeline.run();
    }
}
