package com.dstillery.dataflow.bidstream.ttd;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;

public interface ProcessingOptions extends PipelineOptions {

    /**
     * Set inputFile required parameter to a file path or glob. A local path and Google Cloud
     * Storage path are both supported.
     */
    @Description(
            "Set inputFile required parameter to a file path or glob. A local path and Google Cloud "
                    + "Storage path are both supported.")
    @Validation.Required
    ValueProvider<String> getInputFile();

    void setInputFile(ValueProvider<String> value);

    /**
     * Set output required parameter to define output path. A local path and Google Cloud Storage
     * path are both supported.
     */
    @Description(
            "Set output required parameter to define output path. A local path and Google Cloud Storage"
                    + " path are both supported.")
    @Validation.Required
    ValueProvider<String> getOutput();

    void setOutput(ValueProvider<String> value);

    /**
     * Set whether the input files are coming from Google Cloud Storage. Default value is false.
     * This value is used to determine whether to filter files based on GCS object metadata which is
     * only applicable for files stored in GCS.
     */
    @Description(
            "Set whether the input files are coming from Google Cloud Storage. Default value is true." +
                    "This value is used to determine whether to filter files based on GCS object metadata which is only" +
                    "applicable for files stored in GCS."
    )
    @Default.Boolean(true)
    ValueProvider<Boolean> getUseGcsSource();
    void setUseGcsSource(ValueProvider<Boolean> flag);

    @Description("The Avro Write Temporary Directory. Must end with /")
    @Required
    ValueProvider<String> getAvroTempDirectory();

    void setAvroTempDirectory(ValueProvider<String> value);
}
