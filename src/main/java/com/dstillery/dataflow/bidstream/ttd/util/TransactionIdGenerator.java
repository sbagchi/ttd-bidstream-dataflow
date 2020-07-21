package com.dstillery.dataflow.bidstream.ttd.util;

import java.io.Serializable;
import java.util.UUID;

/**
 * Class that generates a random transaction id.
 */
public class TransactionIdGenerator implements Serializable {
    private static final long serialVersionUID = 7212667572764617538L;
    public String generate() {
        return UUID.randomUUID().toString();
    }
}
