package com.dstillery.dataflow.bidstream.ttd.util;

import org.joda.time.DateTimeFieldType;
import org.joda.time.Instant;

public class BidstreamUtils {
    private static final String PARTITION = "process_date_z=%04d%02d%02d/";

    /**
     * Creates a String that serves as a partition subdirectory when creating files.
     *
     * @param timestamp epoch based timestamp
     * @return String
     */
    public static String dayPartitionName(Long timestamp) {
        Instant instant = new Instant(timestamp);
        return String.format(PARTITION,
                instant.get(DateTimeFieldType.year()),
                instant.get(DateTimeFieldType.monthOfYear()),
                instant.get(DateTimeFieldType.dayOfMonth()));
    }
}
