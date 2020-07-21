package com.dstillery.dataflow.bidstream.ttd.bidrecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BidRecordToGenericRecordConverter is an Apache Beam DoFn that can create a Avro GenericRecord
 * from the BidRecord.
 * This is needed because we use ParquetIO file writer which deals with GenericRecord objects.
 */
public class BidRecordToGenericRecordConverter extends DoFn<BidRecord, GenericRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BidRecordToGenericRecordConverter.class);

    private static final Counter convertedRecords = Metrics.counter(BidRecordToGenericRecordConverter.class, "converted_generic_record");
    private static final Counter unConvertedRecords = Metrics.counter(BidRecordToGenericRecordConverter.class, "failed_converted_generic_record");
    private Schema schema;
    private ReflectDatumWriter<BidRecord> datumWriter;
    private DatumReader<GenericRecord> datumReader;

    @Setup
    public void setup() {
        schema = ReflectData.AllowNull.get().getSchema(BidRecord.class);
        datumWriter = new ReflectDatumWriter<>(schema);
        datumReader = new GenericDatumReader<>(schema);
    }

    @ProcessElement
    public void processElement(@Element BidRecord bidRecord,
                               OutputReceiver<GenericRecord> receiver) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            datumWriter.write(bidRecord, encoder);
            encoder.flush();

            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(outputStream.toByteArray(), null);
            GenericRecord genericRecord = datumReader.read(null, decoder);
            receiver.outputWithTimestamp(genericRecord, Instant.ofEpochMilli((Long)(genericRecord.get("processedMilliSeconds"))));
            convertedRecords.inc();
        } catch (IOException e) {
            unConvertedRecords.inc();
        }
    }
}
