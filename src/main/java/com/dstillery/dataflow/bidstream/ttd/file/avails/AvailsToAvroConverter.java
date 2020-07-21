package com.dstillery.dataflow.bidstream.ttd.file.avails;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang.StringUtils.isNumeric;

import java.time.Clock;
import java.util.Collections;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dstillery.dataflow.bidstream.ttd.bidrecord.BidRecord;
import com.dstillery.dataflow.bidstream.ttd.device.DeviceIdAndType;
import com.dstillery.dataflow.bidstream.ttd.device.DeviceType;
import com.dstillery.dataflow.bidstream.ttd.parser.IpAddressBytesParser;
import com.dstillery.dataflow.bidstream.ttd.parser.UuidBytesParser;
import com.dstillery.dataflow.bidstream.ttd.util.TransactionIdGenerator;
import com.media6.util.IpHasher;
import com.thetradedesk.availsstream.TtdAvails.AvailsLog;

/**
 * Apache Beam DoFn function that takes a protobuf AvailsLog from TTD and converts it to
 * BidRecord which is an Avro POJO that can be used for further processing.
 *
 * Avro is chosen as the format for in-memory representation of our main processing data.
 */
public class AvailsToAvroConverter extends DoFn<AvailsLog, BidRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AvailsToAvroConverter.class);
    protected static final int BRQ_EVENT_TYPE = 10;
    protected static final int PARTNER_ID = 300;
    protected static final int FILE_SOURCE_ID = 34;
    protected static final String ENV_TYPE_WEB = "WEB";
    protected static final String ENV_TYPE_APP = "APP";
    protected static final int IN_APP_RENDERING_CONTEXT = 1;
    private final IpAddressBytesParser ipAddressBytesParser;
    private final UuidBytesParser uuidBytesParser;
    private final TransactionIdGenerator transactionIdGenerator;
    private Clock clock;

    private static final Counter convertedRecords = Metrics.counter(AvailsToAvroConverter.class, "converted_avro_records");

    public AvailsToAvroConverter(IpAddressBytesParser ipAddressBytesParser,
                                 UuidBytesParser uuidBytesParser,
                                 TransactionIdGenerator transactionIdGenerator,
                                 Clock clock) {
        this.ipAddressBytesParser = requireNonNull(ipAddressBytesParser);
        this.uuidBytesParser = requireNonNull(uuidBytesParser);
        this.transactionIdGenerator = requireNonNull(transactionIdGenerator);
        this.clock = requireNonNull(clock);
    }

    @ProcessElement
    public void processElement(@Element AvailsLog record,
                               OutputReceiver<BidRecord> receiver){

        String ipAddress = null;
        if (record.hasIpAddressBytes()) {
            ipAddress =
                    ipAddressBytesParser.parseBytes(record.getIpAddressBytes().toByteArray());
        }

        BidRecord bidRecord = new BidRecord();
        bidRecord.setPartnerId(PARTNER_ID);
        bidRecord.setFileSourceId(FILE_SOURCE_ID);
        bidRecord.setEventId(transactionIdGenerator.generate());
        long epochMillis = clock.millis();
        bidRecord.setProcessedMilliSeconds(epochMillis);
        bidRecord.setPayload(record.toString());

        bidRecord.setEventSeconds(record.getTimeStamp()/1000L);
        bidRecord.setCountry(record.getCountry());
        bidRecord.setEventType(BRQ_EVENT_TYPE);
        bidRecord.setUserAgent(record.getUserAgent());
        bidRecord.setHashedIp(IpHasher.hashIpAddressForLogs(ipAddress));
        DeviceIdAndType bestDevice = getBestDevice(record);

        if (record.hasRenderingContext()) {
            if (record.getRenderingContext() == IN_APP_RENDERING_CONTEXT) {
                bidRecord.setEnvType(ENV_TYPE_APP);
            } else {
                bidRecord.setEnvType(ENV_TYPE_WEB);
            }
        }

        if (bestDevice == null) {
            bidRecord.setDeviceList(Collections.emptyList());
        } else {
            bidRecord.setBestDevice(bestDevice);
            bidRecord.setDeviceList(Collections.singletonList(bidRecord.getBestDevice()));
        }
        LOGGER.debug("bidRecord {}", bidRecord);
        receiver.outputWithTimestamp(bidRecord, Instant.ofEpochMilli(epochMillis));
        convertedRecords.inc();
    }

    private DeviceIdAndType getBestDevice(AvailsLog record) {
        DeviceIdAndType deviceIdAndType = null;
        if (record.hasTdidGuidBytes()) {
            String ttdId =
                    uuidBytesParser.parseBytes(record.getTdidGuidBytes().toByteArray());
            if (ttdId != null) {
                deviceIdAndType = new DeviceIdAndType();
                deviceIdAndType.setId(ttdId);
                deviceIdAndType.setType(DeviceType.SEID);
            }
        } else if (record.hasDeviceIdGuidBytes()) {
            String deviceId =
                    uuidBytesParser.parseBytes(record.getDeviceIdGuidBytes().toByteArray());
            if (deviceId != null) {
                deviceIdAndType = new DeviceIdAndType();
                deviceIdAndType.setId(deviceId);
                deviceIdAndType.setType(getAppDeviceType(record));
            }
        }
        return deviceIdAndType;
    }

    private DeviceType getAppDeviceType(AvailsLog record) {
        if (record.hasDeviceMake() && record.getDeviceMake().equals("Apple")) {
            return DeviceType.IDFA;
        } else {
            if (record.getUrlsCount() > 0) {
                String url = record.getUrls(0);
                if (!isNumeric(url) && !url.startsWith("http")) {
                    return DeviceType.G_IDFA;
                }
            }

        }
        return null;
    }
}
