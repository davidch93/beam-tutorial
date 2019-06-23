package com.dch.tutorial.beam.transform;

import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * A {@link FileIO.Write.FileNaming} that group a collection of files into a date partitioned folder.
 * It means, all streams arrived at a time should be stored in a folder named as yyyy/mm/dd of that time.
 *
 * @author david.christianto
 * @see org.apache.beam.sdk.io.FileIO.Write.FileNaming
 */
public class DatePartitionedFileNaming implements FileIO.Write.FileNaming {

    private static final DateTimeFormatter DATE_PARTITIONED_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
    private static final DateTimeFormatter HOUR_PARTITIONED_FORMATTER = DateTimeFormat.forPattern("HH");

    /**
     * A convenience way to construct {@link DatePartitionedFileNaming}.
     *
     * @return {@link DatePartitionedFileNaming}
     */
    public static DatePartitionedFileNaming withNaming() {
        return new DatePartitionedFileNaming();
    }

    @Override
    public String getFilename(BoundedWindow window, PaneInfo paneInfo, int numShards, int shardIndex,
                              Compression compression) {
        return String.format("%s/part-%s-of-%s-pane-%s%s", filenamePrefixForWindow((IntervalWindow) window), shardIndex,
                numShards, paneInfo.getIndex(), ".parquet");
    }

    /**
     * Method to create filename prefix for each window.
     *
     * @param window {@link IntervalWindow}
     * @return Filename prefix.
     */
    private String filenamePrefixForWindow(IntervalWindow window) {
        return String.format("date_partition=%s/hour_partition=%s",
                DATE_PARTITIONED_FORMATTER.print(window.start()),
                HOUR_PARTITIONED_FORMATTER.print(window.start()));
    }
}
