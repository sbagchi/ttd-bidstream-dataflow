package com.dstillery.dataflow.bidstream.ttd.file.ttd;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.time.Clock;

import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.thetradedesk.availsstream.TtdAvails.AvailsLog;

/**
 * TTDFileParser is a Apache Beam PTransform that can read a collection of TTD files and
 * extract a collection of records from them. One file can produce multiple records.
 */
public class TTDFileParser extends PTransform<PCollection<ReadableFile>, PCollection<AvailsLog>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TTDFileParser.class);

    private final Counter parsedRecords = Metrics.counter(TTDFileParser.class, "parsed_ttd_protobuf_records");
    private final Counter unParsedRecords = Metrics.counter(TTDFileParser.class, "failed_parsed_ttd_protobuf_records");
    private final Counter failedFiles = Metrics.counter(TTDFileParser.class, "failed_ttd_files");

    private static final int INT_SIZE = 4;
    private Clock clock;
    public TTDFileParser(Clock clock) {
        this.clock = clock;
    }

    @Override
    public PCollection<AvailsLog> expand(PCollection<ReadableFile> files) {
        // There are multiple files, each file has multiple bid records
        PCollection<AvailsLog> records = files.apply(ParDo.of(new ExtractBidsFromFileFn(clock)));
        return records;
    }

    class ExtractBidsFromFileFn extends DoFn<ReadableFile, AvailsLog> {
        private Clock clock;
        public ExtractBidsFromFileFn(Clock clock) {
            this.clock = clock;
        }
        @ProcessElement
        public void processElement(@Element ReadableFile element, OutputReceiver<AvailsLog> receiver) {
            LOGGER.debug("file {}", element.getMetadata().resourceId().getFilename());
            try {
                ReadableByteChannel channel = openFile(element);

                ByteBuffer nextEventSizeBuf = ByteBuffer.allocate(INT_SIZE);
                int nextEventSize = getNextEventSize(channel, nextEventSizeBuf);

                while (nextEventSize != -1) {
                    ByteBuffer eventBuf = getNextEvent(channel, nextEventSize);

                    AvailsLog availsLogEvent;
                    try {
                        availsLogEvent = AvailsLog.parseFrom(eventBuf.array());
                    } catch (InvalidProtocolBufferException e) {
                        LOGGER.error("TTD Protobuf Record failed to parse, file {}",
                                element.getMetadata().resourceId().getFilename(), e);
                        unParsedRecords.inc();
                        continue;
                    }
                    parsedRecords.inc();

                    if (!availsLogEvent.getDoNotTrack()) {
                        LOGGER.debug("availsLogEvent {}", availsLogEvent);
                        receiver.outputWithTimestamp(availsLogEvent, Instant.ofEpochMilli(clock.millis()));
                    }

                    nextEventSizeBuf.clear();
                    nextEventSize = getNextEventSize(channel, nextEventSizeBuf);
                }
            } catch (RuntimeException e) {
                LOGGER.error("Error while reading file '{}': '{}'",
                        element.getMetadata().resourceId().getFilename(), e);
                failedFiles.inc();
                return;
            }
        }

        private ReadableByteChannel openFile(ReadableFile file) {
            ReadableByteChannel channel;
            try {
                channel = file.open();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return channel;
        }

        /**
         *
         * @param channel
         * @param nextEventSizeBuf
         * @return size of next event, -1 if reached end of file stream
         */
        private int getNextEventSize(ReadableByteChannel channel, ByteBuffer nextEventSizeBuf) {
            try {
                if (channel.read(nextEventSizeBuf) != -1) {
                    nextEventSizeBuf.flip();
                    nextEventSizeBuf.order(ByteOrder.LITTLE_ENDIAN);
                    return nextEventSizeBuf.getInt();
                }
                return -1;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private ByteBuffer getNextEvent(ReadableByteChannel channel, int nextEventSize) {
            ByteBuffer eventBuf = ByteBuffer.allocate(nextEventSize);
            try {
                channel.read(eventBuf);
                eventBuf.flip();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return eventBuf;
        }
    }
}
