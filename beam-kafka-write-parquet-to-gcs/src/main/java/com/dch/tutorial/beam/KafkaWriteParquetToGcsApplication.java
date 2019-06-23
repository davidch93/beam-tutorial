package com.dch.tutorial.beam;

import com.dch.tutorial.beam.option.WriteParquetOptions;
import com.dch.tutorial.beam.transform.DatePartitionedFileNaming;
import com.dch.tutorial.beam.policy.EventTimePolicy;
import com.dch.tutorial.beam.transform.KafkaRecordTransformer;
import com.dch.tutorial.beam.transform.TombStoneRecordFilter;
import com.dch.tutorial.beam.util.TimeUtil;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Apache Beam streaming application from Kafka, transform the record, and store it to GCS with parquet format.
 *
 * @author David.Christianto
 */
public class KafkaWriteParquetToGcsApplication {

    public static void main(String... args) {
        WriteParquetOptions options = PipelineOptionsFactory.fromArgs(args).as(WriteParquetOptions.class);
        options.setJobName("kafka-write-parquet-to-gcs-job");

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
                .setCoder(AvroCoder.of(GenericRecord.class, KafkaRecordTransformer.SCHEMA))
                .apply("Write Parquet(s) to GCS",
                        FileIO.<GenericRecord>write()
                                .via(ParquetIO.sink(KafkaRecordTransformer.SCHEMA))
                                .to(options.getOutputDirectory())
                                .withNaming(DatePartitionedFileNaming.withNaming())
                                .withNumShards(options.getNumShards()));
        pipeline.run();
    }
}
