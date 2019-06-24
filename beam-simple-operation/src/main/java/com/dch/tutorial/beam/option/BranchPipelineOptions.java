package com.dch.tutorial.beam.option;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * Interface used to manage options to run multiple pipeline.
 * <b>*Note:</b> this options not use {@link org.apache.beam.sdk.options.ValueProvider} because in this example
 * we not want to set or use this argument at runtime.
 *
 * @author david.christianto
 * @see org.apache.beam.sdk.options.PipelineOptions
 */
public interface BranchPipelineOptions extends PipelineOptions {

    @Validation.Required
    @Description("List of prefix to log elements. Separated by comma")
    String getPrefixes();

    void setPrefixes(String prefixes);
}
