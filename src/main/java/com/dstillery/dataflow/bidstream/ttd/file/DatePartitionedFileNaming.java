package com.dstillery.dataflow.bidstream.ttd.file;

import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DatePartitionedFileNaming implements FileIO.Write.FileNaming {
    private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormat.forPattern("yyyyMMddHHmmss");

    private String filePrefix;

    public DatePartitionedFileNaming(String prefix) {
        filePrefix = prefix;
    }

    @Override
    public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
        IntervalWindow intervalWindow = (IntervalWindow) window;
        String fileName = String.format(
                "%s%s-%s-%s-%s%s.parquet",
                filePrefix,
                DATE_TIME_FORMAT.print(intervalWindow.start()),
                DATE_TIME_FORMAT.print(intervalWindow.end()),
                shardIndex,
                numShards,
                compression.getSuggestedSuffix());
        return fileName;
    }
}
