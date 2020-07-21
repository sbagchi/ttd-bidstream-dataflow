package com.dstillery.dataflow.bidstream.ttd.file.ttd;

import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dstillery.throttler.Throttler;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

/**
 * Apache Beam function to apply filter on whether a file is already processed by the pipeline or not.
 * It determines this fact based on a Google Cloud Storage bucket metadata that is set during file read.
 * It checks whether this file metadata is present, if present it skips the file.
 */
public class TtdFileFilter extends DoFn<FileIO.ReadableFile, FileIO.ReadableFile> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TtdFileFilter.class);
    private static final String FILE_READ_KEY = "ttdBidstreamProcessed";
    private static final String FILE_READ_VALUE = "true";
    private final Counter throttledFiles = Metrics.counter(TtdFileFilter.class, "file_throttled");
    private final Counter parsedFiles = Metrics.counter(TtdFileFilter.class, "file_parsed");
    private final boolean shouldUseGCSSource;
    private final Throttler throttler;
    transient Storage storage;

    public TtdFileFilter(Throttler throttler, boolean shouldUseGCSSource) {
        this.throttler = throttler;
        this.shouldUseGCSSource = shouldUseGCSSource;
    }

    @Setup
    public void setup() {
        storage = StorageOptions.getDefaultInstance().getService();
    }

    @ProcessElement
    public void processElement(@Element FileIO.ReadableFile file, OutputReceiver<FileIO.ReadableFile> output) {
        if (!throttler.isAllowed()) {
            throttledFiles.inc();
            return;
        }

        parsedFiles.inc();

        if (!shouldUseGCSSource) {
            output.output(file);
            return;
        }

        GcsPath gcsPath = GcsPath.fromUri(file.getMetadata().resourceId().toString());
        Blob fileObj = storage.get(gcsPath.getBucket(), gcsPath.getObject(), Storage.BlobGetOption.fields(Storage.BlobField.values()));

        if (!fileWasRead(fileObj)) {
            markFileAsRead(fileObj);
            output.output(file);
            LOGGER.debug("'{}' was marked as read", gcsPath);
        } else {
            LOGGER.debug("'{}' was already read in previous run", gcsPath);
        }
    }

    private boolean fileWasRead(Blob file) {
        Map<String, String> metadata = file.getMetadata();
        return (metadata != null && metadata.containsKey(FILE_READ_KEY));
    }

    private void markFileAsRead(Blob file) {
        Map newMetaData = new HashMap();
        newMetaData.put(FILE_READ_KEY, FILE_READ_VALUE);
        file.toBuilder().setMetadata(newMetaData).build().update();
    }
}