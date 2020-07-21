package com.dstillery.dataflow.bidstream.ttd.device;

public enum DeviceType {
    UNKNOWN(0, 0),
    ANDROID_ID(1, 1),
    IMEI(2, 2),
    IDFA(3, 3),
    ODIN(4, 4),
    OPEN_UDID(5, 5),
    ANDROID_ID_MD5(6, 6),
    ANDROID_ID_SHA1(7, 7),
    IDFA_MD5(8, 8),
    IDFA_SHA1(9, 9),
    OPEN_UDID_MD5(10, 10),
    OPEN_UDID_SHA1(11, 11),
    IMEI_MD5(12, 12),
    IMEI_SHA1(13, 13),
    MAC_ADD(14, 14),
    MAC_ADD_MD5(15, 15),
    ATT_AD_WORKS(16, 16),
    VERIZON_UID(17, 17),
    DID_SHA1(18, 18),
    DID_MD5(19, 19),
    DPID_SHA1(20, 20),
    DPID_MD5(21, 21),
    DISPLAY(22, 22),
    CLID(23, 23),
    SEID(24, 24),
    G_IDFA(25, 25),
    IP_GEN_ID(26, 26);

    private final int index;
    private final int value;

    private DeviceType(int index, int value) {
        this.index = index;
        this.value = value;
    }

    public final int getNumber() {
        return this.value;
    }
}
