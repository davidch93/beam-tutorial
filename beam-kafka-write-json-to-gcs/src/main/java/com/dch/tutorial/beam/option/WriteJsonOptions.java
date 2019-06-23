package com.dch.tutorial.beam.option;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Interface used to manage options for write JSON pipeline.
 *
 * @author david.christianto
 * @see org.apache.beam.sdk.options.PipelineOptions
 */
public interface WriteJsonOptions extends PipelineOptions {

    @Validation.Required
    @Description("Kafka Bootstrap Servers")
    ValueProvider<String> getBootstrapServers();

    void setBootstrapServers(ValueProvider<String> bootstrapServers);

    @Validation.Required
    @Description("Kafka group to read the input from")
    ValueProvider<String> getGroupId();

    void setGroupId(ValueProvider<String> groupId);

    @Validation.Required
    @Description("Kafka topic to read the input from")
    ValueProvider<String> getTopic();

    void setTopic(ValueProvider<String> topic);

    @Validation.Required
    @Description("Kafka auto offset reset consumer config")
    ValueProvider<String> getAutoOffsetReset();

    void setAutoOffsetReset(ValueProvider<String> autoOffsetReset);

    @Validation.Required
    @Description("Window the messages into time intervals specified by the executor")
    ValueProvider<String> getWindowDuration();

    void setWindowDuration(ValueProvider<String> windowDuration);

    @Validation.Required
    @Description("Window the messages lateness into time intervals")
    ValueProvider<String> getLateDuration();

    void setLateDuration(ValueProvider<String> lateDuration);

    @Validation.Required
    @Description("Number of shards files")
    ValueProvider<Integer> getNumShards();

    void setNumShards(ValueProvider<Integer> numShards);

    @Validation.Required
    @Description("Output directory in GCS")
    ValueProvider<String> getOutputDirectory();

    void setOutputDirectory(ValueProvider<String> outputDirectory);
}
