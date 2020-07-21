package com.dstillery.dataflow.bidstream.ttd.parser;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class IpAddressBytesParserTest {
    private static final byte[] BYTES = new byte[] {0x0A, 0x0B, 0x0C, 0x0D};
    private static final String EXPECTED_IP_ADDRESS = "10.11.12.13";

    @Mock
    private IpAddressBytesParser ipAddressBytesParser;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        ipAddressBytesParser = new IpAddressBytesParser();
    }

    @Test
    public void parseBytes() {
        assertThat(ipAddressBytesParser.parseBytes(BYTES), is(EXPECTED_IP_ADDRESS));
    }

    @Test
    public void parseNullBytes() {
        assertNull(ipAddressBytesParser.parseBytes(null));
    }

    @Test
    public void parseWrongSizeBytes() {
        assertNull(ipAddressBytesParser.parseBytes(new byte[5]));
    }
}