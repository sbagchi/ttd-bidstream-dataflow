package com.dstillery.dataflow.bidstream.ttd.parser;

import java.io.Serializable;
import java.util.IllegalFormatException;
import java.util.function.Function;

public class UuidBytesParser implements Serializable {

    private static final long serialVersionUID = 3729497582691436303L;

    private static final Function<Byte, String> HEX = (b) -> String.format("%02X", 0xff & b);

    public UuidBytesParser() {
    }

    // The first three groups of bytes are decoded backwards
    // The last two groups of bytes are decoded forwards
    public String parseBytes(byte[] bytes) {
        if (bytes == null || bytes.length != 16) {
            //recordInvalidUuid();
            return null;
        }

        try {
            StringBuilder sb = new StringBuilder();

            for (int i = 3; i >= 0; i--) {
                sb.append(HEX.apply(bytes[i]));
            }
            sb.append('-');

            sb.append(HEX.apply(bytes[5]));
            sb.append(HEX.apply(bytes[4]));
            sb.append('-');

            sb.append(HEX.apply(bytes[7]));
            sb.append(HEX.apply(bytes[6]));
            sb.append('-');

            sb.append(HEX.apply(bytes[8]));
            sb.append(HEX.apply(bytes[9]));
            sb.append('-');

            for (int i = 10; i < 16; i++) {
                sb.append(HEX.apply(bytes[i]));
            }

            return sb.toString().toLowerCase();
        } catch (IllegalFormatException e) {
            return null;
        }
    }
}
