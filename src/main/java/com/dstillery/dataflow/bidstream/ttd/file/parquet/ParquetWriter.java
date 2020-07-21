package com.dstillery.dataflow.bidstream.ttd.file.parquet;

import static com.dstillery.dataflow.bidstream.ttd.util.BidstreamUtils.dayPartitionName;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;

import com.dstillery.dataflow.bidstream.ttd.file.DatePartitionedFileNaming;

/**
 * A utility class that can write Parquet files to a given destination.
 * This is used in the last step of the pipeline where records are written to GCS bucket
 * and later exposed via Hive/BigQuery.
 */
public class ParquetWriter {

    public static PTransform getTransform(Schema schema,
                                          ValueProvider<String> outputPath,
                                          int numShards) {
        return FileIO.<String, GenericRecord>writeDynamic()
                .by(record -> {
                    Long timestamp = (Long) (record.get("processedMilliSeconds"));
                    return dayPartitionName(timestamp);
                })
                .withDestinationCoder(StringUtf8Coder.of())
                .via(ParquetIO.sink(schema))
                .to(outputPath.get())
                .withNumShards(numShards)
                .withNaming(DatePartitionedFileNaming::new);
    }
}
