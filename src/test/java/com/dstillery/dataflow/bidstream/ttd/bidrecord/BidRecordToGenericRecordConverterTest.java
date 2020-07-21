package com.dstillery.dataflow.bidstream.ttd.bidrecord;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;

public class BidRecordToGenericRecordConverterTest {
    private PCollection<BidRecord> inputCollection;
    private PCollection<GenericRecord> outputCollection;
    private List<GenericRecord> expectedOutputCollection;
    private Schema schema = ReflectData.AllowNull.get().getSchema(BidRecord.class);
    private BidRecord bidRecord1;
    private GenericRecord expectedGenericRecord1;
    private BidRecord bidRecord2;
    private GenericRecord expectedGenericRecord2;

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testConversion() {
        givenBidRecord();

        whenPipelineExecuted();

        thenRecordsConverted();
    }

    private void thenRecordsConverted() {
        expectedOutputCollection = Lists.newArrayList(expectedGenericRecord1, expectedGenericRecord2);
        PAssert.that(outputCollection).containsInAnyOrder(expectedOutputCollection);

        pipeline.run().waitUntilFinish();
    }

    private void whenPipelineExecuted() {
        inputCollection = pipeline.apply(Create.of(bidRecord1, bidRecord2));
        outputCollection = inputCollection.apply(ParDo.of(new BidRecordToGenericRecordConverter()))
                .setCoder(AvroCoder.of(GenericRecord.class, schema));
    }

    private void givenBidRecord() {
        bidRecord1 = new BidRecord();
        bidRecord1.setEventId("abc");
        bidRecord1.setProcessedMilliSeconds(100L);
        bidRecord1.setPartnerId(200);
        bidRecord1.setUserAgent("useragent1");

        expectedGenericRecord1 = new GenericData.Record(schema);
        expectedGenericRecord1.put("eventId", "abc");
        expectedGenericRecord1.put("processedMilliSeconds", 100L);
        expectedGenericRecord1.put("partnerId", 200);
        expectedGenericRecord1.put("userAgent", "useragent1");

        bidRecord2 = new BidRecord();
        bidRecord2.setEventId("def");
        bidRecord2.setProcessedMilliSeconds(200L);
        bidRecord2.setPartnerId(300);
        bidRecord2.setUserAgent("useragent2");

        expectedGenericRecord2 = new GenericData.Record(schema);
        expectedGenericRecord2.put("eventId", "def");
        expectedGenericRecord2.put("processedMilliSeconds", 200L);
        expectedGenericRecord2.put("partnerId", 300);
        expectedGenericRecord2.put("userAgent", "useragent2");
    }
}