package com.dstillery.dataflow.bidstream.ttd.file.avails;

import static com.dstillery.dataflow.bidstream.ttd.file.avails.AvailsToAvroConverter.BRQ_EVENT_TYPE;
import static com.dstillery.dataflow.bidstream.ttd.file.avails.AvailsToAvroConverter.ENV_TYPE_APP;
import static com.dstillery.dataflow.bidstream.ttd.file.avails.AvailsToAvroConverter.ENV_TYPE_WEB;
import static com.dstillery.dataflow.bidstream.ttd.file.avails.AvailsToAvroConverter.FILE_SOURCE_ID;
import static com.dstillery.dataflow.bidstream.ttd.file.avails.AvailsToAvroConverter.PARTNER_ID;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.dstillery.dataflow.bidstream.ttd.bidrecord.BidRecord;
import com.dstillery.dataflow.bidstream.ttd.device.DeviceIdAndType;
import com.dstillery.dataflow.bidstream.ttd.device.DeviceType;
import com.dstillery.dataflow.bidstream.ttd.parser.IpAddressBytesParser;
import com.dstillery.dataflow.bidstream.ttd.parser.UuidBytesParser;
import com.dstillery.dataflow.bidstream.ttd.util.TransactionIdGenerator;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.media6.util.IpHasher;
import com.thetradedesk.availsstream.TtdAvails.AvailsLog;

public class AvailsToAvroConverterTest {
    private static final String FIXED_INSTANT = "2020-06-29T10:00:00Z";
    private static final byte[] IP_ADDRESS_BYTES = new byte[] {0x0A, 0x0B, 0x0C, 0x0D};
    private static final String EXPECTED_IP_ADDRESS = "10.11.12.13";
    private static final byte[] TTD_ID_BYTES = new byte[] {0x0, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F};
    private static final String EXPECTED_TTD_ID = "03020100-0504-0706-0809-0a0b0c0d0e0f";
    private static final String TRANSACTION_1 = "transaction1";
    private static final String COUNTRY = "US";
    private static final String USERAGENT_1 = "useragent1";
    private static final String USERAGENT_2 = "useragent2";
    private static final String APPLE = "Apple";

    private Clock clock;
    @Mock(serializable = true)
    private TransactionIdGenerator transactionIdGenerator;
    @Mock(serializable = true)
    private IpAddressBytesParser ipAddressBytesParser;
    @Mock(serializable = true)
    private UuidBytesParser uuidBytesParser;
    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    private PCollection<AvailsLog> inputCollection;
    private PCollection<BidRecord> outputCollection;
    private List<BidRecord> expectedOutputCollection;
    private AvailsLog rawEntry1;
    private BidRecord convertedEntry1;
    private AvailsLog rawEntry2;
    private BidRecord convertedEntry2;

    @Before
    public void setup() {
        clock = Clock.fixed(Instant.parse(FIXED_INSTANT), ZoneOffset.UTC);
        MockitoAnnotations.initMocks(this);
        when(transactionIdGenerator.generate()).thenReturn(TRANSACTION_1);
        when(ipAddressBytesParser.parseBytes(IP_ADDRESS_BYTES)).thenReturn(EXPECTED_IP_ADDRESS);
        when(uuidBytesParser.parseBytes(TTD_ID_BYTES)).thenReturn(EXPECTED_TTD_ID);
    }

    @Test
    public void testConversion() {

        givenRawEntries();

        whenPipelineExecuted();

        thenRecordsConverted();
    }

    private void thenRecordsConverted() {
        expectedOutputCollection = Lists.newArrayList(convertedEntry1, convertedEntry2);
        PAssert.that(outputCollection).containsInAnyOrder(expectedOutputCollection);

        pipeline.run().waitUntilFinish();
    }

    private void whenPipelineExecuted() {
        inputCollection = pipeline.apply(Create.of(rawEntry1, rawEntry2));
        outputCollection = inputCollection.apply(ParDo.of(new AvailsToAvroConverter(ipAddressBytesParser, uuidBytesParser, transactionIdGenerator, clock)));
    }

    private void givenRawEntries() {
        rawEntry1 =
                AvailsLog.newBuilder()
                        .setSupplyVendorId(1)
                        .setTimeStamp(clock.millis())
                        .setCountry(COUNTRY)
                        .setUserAgent(USERAGENT_1)
                        .setRenderingContext(3)
                        .setTdidGuidBytes(ByteString.copyFrom(TTD_ID_BYTES))
                        .setIpAddressBytes(ByteString.copyFrom(IP_ADDRESS_BYTES))
                        .build();
        convertedEntry1 = new BidRecord();
        convertedEntry1.setEventSeconds(clock.millis()/1000L);
        convertedEntry1.setProcessedMilliSeconds(clock.millis());
        convertedEntry1.setCountry(COUNTRY);
        convertedEntry1.setUserAgent(USERAGENT_1);
        convertedEntry1.setEnvType(ENV_TYPE_WEB);
        convertedEntry1.setPartnerId(PARTNER_ID);
        convertedEntry1.setFileSourceId(FILE_SOURCE_ID);
        convertedEntry1.setEventType(BRQ_EVENT_TYPE);
        convertedEntry1.setEventId(TRANSACTION_1);
        convertedEntry1.setHashedIp(IpHasher.hashIpAddressForLogs(EXPECTED_IP_ADDRESS));

        DeviceIdAndType browser = new DeviceIdAndType();
        browser.setType(DeviceType.SEID);
        browser.setId(EXPECTED_TTD_ID);
        convertedEntry1.setBestDevice(browser);
        convertedEntry1.setDeviceList(Lists.newArrayList(browser));
        convertedEntry1.setPayload(rawEntry1.toString());

        rawEntry2 =
                AvailsLog.newBuilder()
                        .setSupplyVendorId(1)
                        .setTimeStamp(clock.millis())
                        .setCountry(COUNTRY)
                        .setUserAgent(USERAGENT_2)
                        .setRenderingContext(1)
                        .setDeviceMake(APPLE)
                        .setDeviceIdGuidBytes(ByteString.copyFrom(TTD_ID_BYTES))
                        .build();
        convertedEntry2 = new BidRecord();
        convertedEntry2.setEventSeconds(clock.millis()/1000L);
        convertedEntry2.setProcessedMilliSeconds(clock.millis());
        convertedEntry2.setCountry(COUNTRY);
        convertedEntry2.setUserAgent(USERAGENT_2);
        convertedEntry2.setEnvType(ENV_TYPE_APP);
        convertedEntry2.setPartnerId(PARTNER_ID);
        convertedEntry2.setFileSourceId(FILE_SOURCE_ID);
        convertedEntry2.setEventType(BRQ_EVENT_TYPE);
        convertedEntry2.setEventId(TRANSACTION_1);
        DeviceIdAndType app = new DeviceIdAndType();
        app.setType(DeviceType.IDFA);
        app.setId(EXPECTED_TTD_ID);
        convertedEntry2.setBestDevice(app);
        convertedEntry2.setDeviceList(Lists.newArrayList(app));
        convertedEntry2.setPayload(rawEntry2.toString());
    }
}