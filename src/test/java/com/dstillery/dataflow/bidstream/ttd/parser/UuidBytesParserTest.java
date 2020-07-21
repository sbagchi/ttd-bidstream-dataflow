package com.dstillery.dataflow.bidstream.ttd.parser;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;

import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

public class UuidBytesParserTest {
    private static final byte[] BYTES = new byte[] {0x0, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F};
    private static final String EXPECTED_UUID = "03020100-0504-0706-0809-0a0b0c0d0e0f";

    private UuidBytesParser uuidBytesParser;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        uuidBytesParser = new UuidBytesParser();
    }

    @Test
    public void parseBytes() {
        assertThat(uuidBytesParser.parseBytes(BYTES), is(EXPECTED_UUID));
    }

    @Test
    public void parseNullBytes() {
        assertNull(uuidBytesParser.parseBytes(null));
    }

    @Test
    public void parseWrongSizeBytes() {
        assertNull(uuidBytesParser.parseBytes(new byte[5]));
    }
}