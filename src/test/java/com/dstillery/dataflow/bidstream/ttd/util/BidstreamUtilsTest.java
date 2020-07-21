package com.dstillery.dataflow.bidstream.ttd.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class BidstreamUtilsTest {

    @Test
    public void dayPartitionName() {
        assertEquals("process_date_z=20200621/",
                BidstreamUtils.dayPartitionName(1592701200000L));

        assertEquals("process_date_z=20191201/",
                BidstreamUtils.dayPartitionName(1575219600000L));

    }
}