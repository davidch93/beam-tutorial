package com.dch.tutorial.beam.policy;

import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nullable;

/**
 * A {@link FileBasedSink.FilenamePolicy} that group a collection of files into a date partitioned folder.
 * It means, all streams arrived at a time should be stored in a folder named as yyyy/mm/dd of that time.
 *
 * @author david.christianto
 */
public class DatePartitionedNamePolicy extends FileBasedSink.FilenamePolicy {

    private static final DateTimeFormatter DATE_PARTITIONED_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
    private static final DateTimeFormatter HOUR_PARTITIONED_FORMATTER = DateTimeFormat.forPattern("HH");

    private final ResourceId outputDirectory;

    private DatePartitionedNamePolicy(ResourceId outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    /**
     * A convenience way to construct {@link DatePartitionedNamePolicy}.
     *
     * @param outputDirectory {@link ResourceId} Folder path prefix.
     * @return {@link DatePartitionedNamePolicy}
     */
    public static DatePartitionedNamePolicy withDatePartitioned(ResourceId outputDirectory) {
        return new DatePartitionedNamePolicy(outputDirectory);
    }

    @Override
    public ResourceId windowedFilename(int shardNumber, int numShards, BoundedWindow window, PaneInfo paneInfo,
                                       FileBasedSink.OutputFileHints outputFileHints) {
        String filename = String.format("%s/part-%s-of-%s-pane-%s%s", filenamePrefixForWindow((IntervalWindow) window),
                shardNumber, numShards, paneInfo.getIndex(), ".json");
        return outputDirectory.getCurrentDirectory()
                .resolve(filename, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
    }

    @Nullable
    @Override
    public ResourceId unwindowedFilename(int shardNumber, int numShards,
                                         FileBasedSink.OutputFileHints outputFileHints) {
        throw new UnsupportedOperationException("Unsupported");
    }

    /**
     * Method to create filename prefix for each window.
     *
     * @param window {@link IntervalWindow}
     * @return Filename prefix.
     */
    private String filenamePrefixForWindow(IntervalWindow window) {
        String directory = outputDirectory.isDirectory() ? "" : outputDirectory.getFilename();
        return String.format("%s/date_partition=%s/hour_partition=%s", directory,
                DATE_PARTITIONED_FORMATTER.print(window.start()),
                HOUR_PARTITIONED_FORMATTER.print(window.start()));
    }
}
