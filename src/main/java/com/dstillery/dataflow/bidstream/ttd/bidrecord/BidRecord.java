package com.dstillery.dataflow.bidstream.ttd.bidrecord;

import java.util.List;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import com.dstillery.classutils.Sidekick;
import com.dstillery.dataflow.bidstream.ttd.device.DeviceIdAndType;

/**
 * Avro POJO to hold the TTD raw bid record and its derived fields.
 */
@DefaultCoder(AvroCoder.class)
public class BidRecord {
    private static final Sidekick<BidRecord> SIDEKICK = Sidekick.newBuilder(BidRecord.class)
            .add("partnerId", BidRecord::getPartnerId)
            .add("fileSourceId", BidRecord::getFileSourceId)
            .add("eventId", BidRecord::getEventId)
            .add("processedMilliSeconds", BidRecord::getProcessedMilliSeconds)
            .add("eventSeconds", BidRecord::getEventSeconds)
            .add("country", BidRecord::getCountry)
            .add("envType", BidRecord::getEnvType)
            .add("eventType", BidRecord::getEventType)
            .add("eventSubType", BidRecord::getEventSubType)
            .add("userAgent", BidRecord::getUserAgent)
            .add("hashedIp", BidRecord::getHashedIp)
            .add("bestDevice", BidRecord::getBestDevice)
            .add("deviceList", BidRecord::getDeviceList)
            .add("payload", BidRecord::getPayload)
            .build();

    @Nullable
    private Integer partnerId;
    @Nullable
    private Integer fileSourceId;
    @Nullable
    private String eventId;
    @Nullable
    private Long processedMilliSeconds;
    @Nullable
    private String payload;
    @Nullable
    private Long eventSeconds;
    @Nullable
    private String country;
    @Nullable
    private String envType;
    @Nullable
    private Integer eventType;
    @Nullable
    private Integer eventSubType;
    @Nullable
    private String userAgent;
    @Nullable
    private String hashedIp;
    @Nullable
    private DeviceIdAndType bestDevice;
    @Nullable
    private List<DeviceIdAndType> deviceList;

    public Integer getPartnerId() {
        return partnerId;
    }

    public void setPartnerId(Integer partnerId) {
        this.partnerId = partnerId;
    }

    public Integer getFileSourceId() {
        return fileSourceId;
    }

    public void setFileSourceId(Integer fileSourceId) {
        this.fileSourceId = fileSourceId;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Long getProcessedMilliSeconds() {
        return processedMilliSeconds;
    }

    public void setProcessedMilliSeconds(Long processedMilliSeconds) {
        this.processedMilliSeconds = processedMilliSeconds;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getEnvType() {
        return envType;
    }

    public void setEnvType(String envType) {
        this.envType = envType;
    }

    public Integer getEventType() {
        return eventType;
    }

    public void setEventType(Integer eventType) {
        this.eventType = eventType;
    }

    public Integer getEventSubType() {
        return eventSubType;
    }

    public void setEventSubType(Integer eventSubType) {
        this.eventSubType = eventSubType;
    }

    public Long getEventSeconds() {
        return eventSeconds;
    }

    public void setEventSeconds(Long eventSeconds) {
        this.eventSeconds = eventSeconds;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public String getHashedIp() {
        return hashedIp;
    }

    public void setHashedIp(String hashedIp) {
        this.hashedIp = hashedIp;
    }

    public DeviceIdAndType getBestDevice() {
        return bestDevice;
    }

    public void setBestDevice(DeviceIdAndType bestDevice) {
        this.bestDevice = bestDevice;
    }

    public List<DeviceIdAndType> getDeviceList() {
        return deviceList;
    }

    public void setDeviceList(List<DeviceIdAndType> deviceList) {
        this.deviceList = deviceList;
    }

    @Override
    public final boolean equals(Object o) {
        return SIDEKICK.kickEquals(this, o);
    }

    @Override
    public final int hashCode() {
        return SIDEKICK.kickHashCode(this);
    }

    @Override
    public String toString() {
        return SIDEKICK.kickToString(this);
    }
}
