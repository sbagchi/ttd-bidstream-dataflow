package com.dstillery.dataflow.bidstream.ttd.parser;

import java.io.Serializable;
import java.util.IllegalFormatException;
import java.util.StringJoiner;
import java.util.function.Function;

public class IpAddressBytesParser implements Serializable {

    private static final long serialVersionUID = 8312767542764217838L;

    private static final Function<Byte, Integer> OCTET_INT = (b) -> Integer.parseInt(String.format("%02X", 0xff & b), 16);

    public IpAddressBytesParser() {
    }

    public String parseBytes(byte[] bytes) {
        if (bytes == null || bytes.length != 4) {
            //recordInvalidIpAddress();
            return null;
        }

        StringJoiner sj = new StringJoiner(".");

        try {
            for (int i = 0; i <= 3; i++) {
                sj.add(OCTET_INT.apply(bytes[i]).toString());
            }
        } catch (IllegalFormatException | NumberFormatException e) {
            //recordInvalidIpAddress();
            return null;
        }

        return sj.toString();
    }
}
