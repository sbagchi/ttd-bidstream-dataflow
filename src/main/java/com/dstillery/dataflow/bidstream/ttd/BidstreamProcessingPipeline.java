package com.dstillery.dataflow.bidstream.ttd;

import java.io.IOException;
import java.time.Clock;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dstillery.dataflow.bidstream.ttd.bidrecord.BidRecord;
import com.dstillery.dataflow.bidstream.ttd.bidrecord.BidRecordToGenericRecordConverter;
import com.dstillery.dataflow.bidstream.ttd.file.avails.AvailsToAvroConverter;
import com.dstillery.dataflow.bidstream.ttd.file.parquet.ParquetWriter;
import com.dstillery.dataflow.bidstream.ttd.file.ttd.TTDFileParser;
import com.dstillery.dataflow.bidstream.ttd.file.ttd.TtdFileFilter;
import com.dstillery.dataflow.bidstream.ttd.parser.IpAddressBytesParser;
import com.dstillery.dataflow.bidstream.ttd.parser.UuidBytesParser;
import com.dstillery.dataflow.bidstream.ttd.util.TransactionIdGenerator;
import com.dstillery.throttler.PercBasedThrottler;
import com.dstillery.throttler.Throttler;
import com.thetradedesk.availsstream.TtdAvails.AvailsLog;

/**
 * Apache Beam pipeline to run TTD Bidstream raw processing on Dataflow
 */
public class BidstreamProcessingPipeline {
    /**
     * Number of shards during file writing, a value of zero means system determines the number of files.
     * Zero is chosen for better performance.
     */
    protected static int NUM_WRITE_SHARDS = 50;

    protected static int THROTTLE_PERCENTAGE = 5;

    /**
     * Apache Beam time based window size
     */
    protected static int DEFAULT_WINDOW_SIZE_IN_MINS = 5;
    private static final Logger LOGGER = LoggerFactory.getLogger(BidstreamProcessingPipeline.class);

    protected static void processRawFiles(ProcessingOptions options) {
        PTransform<PBegin, PCollection<Metadata>> fileInput =
                FileIO.match().filepattern(options.getInputFile());
        processInput(options, fileInput, Clock.systemUTC());
    }

    protected static void processInput(ProcessingOptions options,
                                       PTransform<PBegin, PCollection<MatchResult.Metadata>> input,
                                       Clock clock) {

        Schema schema = ReflectData.AllowNull.get().getSchema(BidRecord.class);

        FileSystems.setDefaultPipelineOptions(options);
        Pipeline pipeline = Pipeline.create(options);

        TransactionIdGenerator transactionIdGenerator = new TransactionIdGenerator();
        IpAddressBytesParser ipAddressBytesParser = new IpAddressBytesParser();
        UuidBytesParser uuidBytesParser = new UuidBytesParser();
        Throttler throttler = new PercBasedThrottler(THROTTLE_PERCENTAGE);
        pipeline.apply(input)
                .apply("Read Files", FileIO.readMatches())
                .apply("Filter processed files", ParDo.of(new TtdFileFilter(throttler, options.getUseGcsSource().get())))
                .apply("Parse into TTD protobuf", new TTDFileParser(clock))
                /* https://cloud.google.com/dataflow/docs/guides/deploying-a-pipeline#Optimization
                 * Reshuffle is supported by Dataflow even though it is marked deprecated in the Apache Beam documentation
                 * */
                //.apply("Apply hourly partitioning", Window.into(FixedWindows.of(Duration.standardMinutes(DEFAULT_WINDOW_SIZE_IN_MINS))))
                //.apply("Reshuffle via random key", Reshuffle.viaRandomKey())
                .apply("Convert to Avro record", ParDo.of(new AvailsToAvroConverter(ipAddressBytesParser, uuidBytesParser, transactionIdGenerator, clock)))
                //.apply("Convert to generic record", ParDo.of(new BidRecordToGenericRecordConverter()))
                //.setCoder(AvroCoder.of(GenericRecord.class, schema))
                //.apply("apply windowing", getWindow())
                .apply("Apply hourly partitioning", Window.into(FixedWindows.of(Duration.standardMinutes(DEFAULT_WINDOW_SIZE_IN_MINS))))
                //.apply("WriteToParquet", ParquetWriter.getTransform(schema, options.getOutput(), NUM_WRITE_SHARDS));
                .apply(
                        "Write File(s)",
                        AvroIO.write(BidRecord.class)
                                .to(
                                        new WindowedFilenamePolicy(
                                                options.getOutput().get(),
                                                "output",
                                                "W-P-SS-of-NN",
                                                ".avro"))
                                .withTempDirectory(NestedValueProvider.of(
                                        options.getAvroTempDirectory(),
                                        (SerializableFunction<String, ResourceId>) x ->
                                                FileBasedSink.convertToFileResourceIfPossible(x)))
                                .withWindowedWrites()
                                .withOutputFilenames()
                                .withNumShards(NUM_WRITE_SHARDS));
        pipeline.run();
    }

    private static Window<BidRecord> getWindow() {
        return Window.<BidRecord>into(FixedWindows.of(Duration.standardMinutes(DEFAULT_WINDOW_SIZE_IN_MINS)))
                .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                .discardingFiredPanes()
                .withAllowedLateness(Duration.ZERO);
    }

    public static void main(String[] args) throws IOException, IllegalArgumentException {
        // Create and setup PipelineOptions.
        PipelineOptionsFactory.register(ProcessingOptions.class);
        ProcessingOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(ProcessingOptions.class);
        LOGGER.info("starting......");
        processRawFiles(options);
    }
}
