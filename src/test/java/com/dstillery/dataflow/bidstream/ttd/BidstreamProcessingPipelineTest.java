package com.dstillery.dataflow.bidstream.ttd;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.ReflectDataSupplier;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import com.dstillery.dataflow.bidstream.ttd.bidrecord.BidRecord;
import com.dstillery.dataflow.bidstream.ttd.device.DeviceType;
import com.media6.util.IpHasher;

@RunWith(JUnit4.class)
public class BidstreamProcessingPipelineTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(BidstreamProcessingPipelineTest.class);
    private static final Integer BRQ_EVENT_TYPE = 10;
    private static final Integer PARTNER_ID = 300;
    private static final Integer FILE_SOURCE_ID = 34;
    private static final String ENV_TYPE_WEB = "WEB";
    private static final String ENV_TYPE_APP = "APP";
    private static final String FIXED_INSTANT = "2020-06-29T10:00:00Z";
    private static final String US = "US";

    private File outputDir;
    private PTransform<PBegin, PCollection<Metadata>> input;
    private Clock clock;
    private List<File> outputPartitions;
    List<BidRecord> outputRecords;

    @Rule
    public TemporaryFolder output = new TemporaryFolder();

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @BeforeClass
    public static void setUp() {
        PipelineOptionsFactory.register(ProcessingOptions.class);
        BidstreamProcessingPipeline.DEFAULT_WINDOW_SIZE_IN_MINS = 60;
        BidstreamProcessingPipeline.NUM_WRITE_SHARDS = 1;
        BidstreamProcessingPipeline.THROTTLE_PERCENTAGE = 100;
    }

    @Test
    public void processRawFiles() throws IOException {
        givenClock();
        ProcessingOptions options = givenPipelineOptions();
        givenInputFileExists(options, "src/test/resources/*.log.gz");
        givenOutputDirectoryExists(options, "result");

        whenPipelineExecuted(options);

        thenOutputPartitionsExist(Arrays.asList("process_date_z=20200629"));

        thenRecordsFound();
    }

    private void givenClock() {
        clock = Clock.fixed(Instant.parse(FIXED_INSTANT), ZoneOffset.UTC);
    }

    private ProcessingOptions givenPipelineOptions() {
        return TestPipeline.testingPipelineOptions().as(ProcessingOptions.class);
    }

    private void givenInputFileExists(ProcessingOptions options, String path) {
        options.setUseGcsSource(StaticValueProvider.of(false));
        options.setInputFile(StaticValueProvider.of(path));
        input = FileIO.match().filepattern(options.getInputFile());
    }

    private void givenOutputDirectoryExists(ProcessingOptions options, String path) throws IOException {
        outputDir = output.newFolder(path);
        options.setOutput(StaticValueProvider.of(outputDir.getAbsolutePath()));
    }

    private void whenPipelineExecuted(ProcessingOptions options) {
        BidstreamProcessingPipeline.processInput(options, input, clock);
        outputPartitions = getSubdirectories(outputDir, "process_date_z=2020");
    }

    private void thenOutputPartitionsExist(List<String> directoriesToExpect) {
        assertEquals(directoriesToExpect, outputPartitions.stream().map(f -> f.getName()).collect(toList()));
    }

    private void thenRecordsFound() throws IOException {
        outputRecords = recordsFromPartition(outputPartitions, "process_date_z=20200629");
        thenRecordMatches(outputRecords.get(0),
                1592701200L, "184.103.89.200",
                "5fd94b9d-7d02-4a79-975b-6d08a5634981", DeviceType.SEID, ENV_TYPE_WEB);
        thenRecordMatches(outputRecords.get(1),
                1592787600L, null,
                "5b237080-3e92-4092-9d4e-a1b748bc6411", DeviceType.G_IDFA, ENV_TYPE_APP);
        thenRecordMatches(outputRecords.get(2),
                1592874000L, "97.126.109.223",
                "0eb342e8-42cd-41c1-bbda-9582a1979741", DeviceType.SEID, ENV_TYPE_WEB);

    }

    private void thenRecordMatches(BidRecord record,
                                   Long eventSeconds,
                                   String ipAddress,
                                   String bestDeviceId,
                                   DeviceType bestDeviceType,
                                   String envType) {
        assertEquals(Long.valueOf(eventSeconds), record.getEventSeconds());
        assertEquals(PARTNER_ID, record.getPartnerId());
        assertEquals(FILE_SOURCE_ID, record.getFileSourceId());
        assertEquals(BRQ_EVENT_TYPE, record.getEventType());
        assertEquals(US, record.getCountry());
        assertEquals(IpHasher.hashIpAddressForLogs(ipAddress), record.getHashedIp());
        assertEquals(bestDeviceId, record.getBestDevice().getId());
        assertEquals(bestDeviceType, record.getBestDevice().getType());
        assertEquals(envType, record.getEnvType());

    }

    /**
     * Extracts the Parquet record from the given file in a given partition.
     * Partitions are managed by date.
     * Example is output/process_date_z=20200621/file.parquet which represents records for 20200621
     * This method will read the content of file.parquet and return it as GenericRecord in Avro format.
     *
     * @param partitions the available partitions
     * @param datePart date partition that is requested
     * @return GenericRecord instance
     * @throws IOException
     */
    private List<BidRecord> recordsFromPartition(List<File> partitions, String datePart) throws IOException {
        File dateDir = partitions.stream()
                .filter(p -> p.isDirectory() && p.getName().equals(datePart))
                .collect(toList()).get(0);
        File dataFile = Arrays.asList(dateDir.listFiles()).get(0);
        return readAvroParquetFile(dataFile);
    }

    private List<BidRecord> readAvroParquetFile(File file) throws IOException {
        Configuration conf = new Configuration(false);
        conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
        AvroReadSupport.setAvroDataSupplier(conf, ReflectDataSupplier.class);

        ParquetReader<BidRecord> reader = new AvroParquetReader<>(conf, new Path(file.getAbsolutePath()));
        List<BidRecord> records = new ArrayList<>();
        BidRecord record;
        while (true) {
            record = reader.read();
            if (record == null) {
                break;
            } else {
                records.add(record);
            }
        }
        records.sort(Comparator.comparing(BidRecord::getEventSeconds));
        return records;
    }

    private List<File> getSubdirectories(File outputDir, String filterPath) {
        List<File> directories = Arrays.asList(outputDir.listFiles(File::isDirectory))
                .stream().filter(f -> f.getName().startsWith(filterPath)).collect(toList());
        directories.sort(Comparator.comparing(File::getName));
        return directories;
    }
}
