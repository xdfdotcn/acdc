/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package cn.xdf.acdc.connect.hdfs.writer;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.format.ProjectedResult;
import cn.xdf.acdc.connect.hdfs.format.RecordWriter;
import cn.xdf.acdc.connect.hdfs.format.RecordWriterProvider;
import cn.xdf.acdc.connect.hdfs.format.SchemaReader;
import cn.xdf.acdc.connect.hdfs.hive.errors.HiveMetaStoreException;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.PartitionerConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.TimeBasedPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.TimestampExtractor;
import cn.xdf.acdc.connect.hdfs.storage.HdfsFileOperator;
import cn.xdf.acdc.connect.hdfs.storage.HdfsStorage;
import cn.xdf.acdc.connect.hdfs.storage.StorageSinkConnectorConfig;
import cn.xdf.acdc.connect.hdfs.storage.schema.StorageSchemaCompatibility;
import cn.xdf.acdc.connect.hdfs.util.DateTimeUtils;
import cn.xdf.acdc.connect.hdfs.wal.FilePathOffset;
import cn.xdf.acdc.connect.hdfs.wal.WAL;
import io.confluent.common.utils.Time;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 Exactly Once Writer.
 */
@Slf4j
public class ExactlyOnceTopicPartitionWriter implements TopicPartitionWriter {

    private static final TimestampExtractor WALLCLOCK =
        new TimeBasedPartitioner.WallclockTimestampExtractor();

    private final RecordWriterProvider writerProvider;

    private final String zeroPadOffsetFormat;

    private final Time time;

    private final HdfsStorage storage;

    private final WAL wal;

    private final Map<String, String> tempFiles;

    private final Map<String, RecordWriter> writers;

    private final TopicPartition topicPartition;

    private final Partitioner partitioner;

    private final TimestampExtractor timestampExtractor;

    private final boolean isWallclockBased;

    private State state;

    private final Queue<SinkRecord> buffer;

    private boolean recovered;

    private final SinkTaskContext context;

    private int recordCounter;

    private final int flushSize;

    private final long rotateIntervalMs;

    private Long lastRotate;

    private final long rotateScheduleIntervalMs;

    private long nextScheduledRotate;

    // This is one case where we cannot simply wrap the old or new RecordWriterProvider with the
    // other because they have incompatible requirements for some methods -- one requires the Hadoop
    // config + extra parameters, the other requires the ConnectorConfig and doesn't get the other
    // extra parameters. Instead, we have to (optionally) store one of each and use whichever one is
    // non-null.
    private final HdfsSinkConfig hdfsSinkConfig;

    private final Set<String> appended;

    private long offset;

    private final Map<String, Long> startOffsets;

    private final Map<String, Long> endOffsets;

    private final long timeoutMs;

    private long failureTime;

    private final StorageSchemaCompatibility compatibility;

    private final String extension;

    private final DateTimeZone timeZone;

    private final SchemaReader schemaReader;

    private final StoreConfig storeConfig;

    private final HdfsFileOperator fileOperator;

    private final HiveMetaRestorer hiveMetaRestorer;

    public ExactlyOnceTopicPartitionWriter(
        final SinkTaskContext context,
        final Time time,
        final TopicPartition topicPartition,
        final StoreContext storeContext
    ) {
        this.time = time;
        this.topicPartition = topicPartition;
        this.context = context;
        this.fileOperator = storeContext.getFileOperator();
        this.storage = fileOperator.storage();
        this.writerProvider = storeContext.getRecordWriterProvider();
        this.storeConfig = storeContext.getStoreConfig();
        this.partitioner = storeContext.getPartitioner();
        this.hiveMetaRestorer = storeContext.getHiveMetaRestorer();
        TimestampExtractor timestampExtractor = null;
        if (TimeBasedPartitioner.class.isAssignableFrom(partitioner.getClass())) {
            timestampExtractor = ((TimeBasedPartitioner) partitioner).getTimestampExtractor();
        }
        this.timestampExtractor = timestampExtractor != null ? timestampExtractor : WALLCLOCK;
        this.isWallclockBased = TimeBasedPartitioner.WallclockTimestampExtractor.class.isAssignableFrom(
            this.timestampExtractor.getClass()
        );
        this.hdfsSinkConfig = storeContext.getHdfsSinkConfig();
        this.schemaReader = storeContext.getSchemaReader();

        flushSize = hdfsSinkConfig.getInt(HdfsSinkConfig.FLUSH_SIZE_CONFIG);
        rotateIntervalMs = hdfsSinkConfig.getLong(HdfsSinkConfig.ROTATE_INTERVAL_MS_CONFIG);
        rotateScheduleIntervalMs = hdfsSinkConfig.getLong(HdfsSinkConfig
            .ROTATE_SCHEDULE_INTERVAL_MS_CONFIG);
        timeoutMs = Long.valueOf(String.valueOf(hdfsSinkConfig.getInt(HdfsSinkConfig.RETRY_BACKOFF_MS)));
        compatibility = StorageSchemaCompatibility.getCompatibility(
            hdfsSinkConfig.getString(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG));
        wal = fileOperator.storage().wal(storeConfig, topicPartition);
        buffer = new LinkedList<>();
        writers = new HashMap<>();
        tempFiles = new HashMap<>();
        appended = new HashSet<>();
        startOffsets = new HashMap<>();
        endOffsets = new HashMap<>();
        state = State.RECOVERY_STARTED;
        failureTime = -1L;
        // The next offset to consume after the last commit (one more than last offset written to HDFS)
        offset = -1L;
        extension = writerProvider.getExtension();
        zeroPadOffsetFormat = "%0"
            + hdfsSinkConfig.getInt(HdfsSinkConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG)
            + "d";

        if (rotateScheduleIntervalMs > 0) {
            timeZone = DateTimeZone.forID(hdfsSinkConfig.getString(PartitionerConfig.TIMEZONE_CONFIG));
        } else {
            timeZone = null;
        }

        // Initialize rotation timers
        updateRotationTimers(null);
    }

    // CHECKSTYLE:OFF
    @SuppressWarnings("fallthrough")
    @Override
    public boolean recover() {
        try {
            switch (state) {
                case RECOVERY_STARTED:
                    log.info("Started recovery for topic partition {}", topicPartition);
                    pause();
                    nextState();
                case RECOVERY_PARTITION_PAUSED:
                    log.debug("Start recovery state: Apply WAL for topic partition {}", topicPartition);
                    applyWAL();
                    nextState();
                case WAL_APPLIED:
                    log.debug("Start recovery state: Reset Offsets for topic partition {}", topicPartition);
                    resetOffsets();
                    nextState();
                case OFFSET_RESET:
                    log.debug("Start recovery state: Truncate WAL for topic partition {}", topicPartition);
                    truncateWAL();
                    nextState();
                case WAL_TRUNCATED:
                    log.debug("Start recovery state: Resume for topic partition {}", topicPartition);
                    resume();
                    nextState();
                    log.info("Finished recovery for topic partition {}", topicPartition);
                    break;
                default:
                    log.error(
                        "{} is not a valid state to perform recovery for topic partition {}.",
                        state,
                            topicPartition
                    );
            }
        } catch (ConnectException e) {
            log.error("Recovery failed at state {}", state, e);
            setRetryTimeout(timeoutMs);
            return false;
        }
        return true;
    }
    // CHECKSTYLE:ON

    private void updateRotationTimers(final SinkRecord currentRecord) {
        long now = time.milliseconds();
        // Wallclock-based partitioners should be independent of the record argument.
        lastRotate = isWallclockBased
            ? (Long) now
            : currentRecord != null ? timestampExtractor.extract(currentRecord) : null;
        if (log.isDebugEnabled() && rotateIntervalMs > 0) {
            log.debug(
                "Update last rotation timer. Next rotation for {} will be in {}ms",
                    topicPartition,
                rotateIntervalMs
            );
        }
        if (rotateScheduleIntervalMs > 0) {
            nextScheduledRotate = DateTimeUtils.getNextTimeAdjustedByDay(
                now,
                rotateScheduleIntervalMs,
                timeZone
            );
            if (log.isDebugEnabled()) {
                log.debug(
                    "Update scheduled rotation timer. Next rotation for {} will be at {}",
                        topicPartition,
                    new DateTime(nextScheduledRotate).withZone(timeZone).toString()
                );
            }
        }
    }

    @SuppressWarnings("fallthrough")
    @Override
    // CHECKSTYLE:OFF
    public void write() {
        long now = time.milliseconds();
        SinkRecord currentRecord = null;
        if (failureTime > 0 && now - failureTime < timeoutMs) {
            return;
        }
        if (state.compareTo(State.WRITE_STARTED) < 0) {
            boolean success = recover();
            if (!success) {
                return;
            }
            updateRotationTimers(null);
        }
        while (!buffer.isEmpty()) {
            try {
                switch (state) {
                    case WRITE_STARTED:
                        pause();
                        nextState();
                    case WRITE_PARTITION_PAUSED:
                        SinkRecord record = buffer.peek();
                        currentRecord = record;
                        // compatibility
                        ProjectedResult projectedResult = schemaReader.projectRecord(topicPartition, record);
                        if (projectedResult.isNeedChangeSchema()) {
                            doChangeSchema(projectedResult.getCurrentSchema());
                            if (recordCounter > 0) {
                                nextState();
                            } else {
                                break;
                            }
                        } else {
                            if (shouldRotateAndMaybeUpdateTimers(currentRecord, now)) {
                                log.info(
                                    "Starting commit and rotation for topic partition {} with start offsets {} "
                                        + "and end offsets {}",
                                        topicPartition,
                                    startOffsets,
                                    endOffsets
                                );
                                nextState();
                                // Fall through and try to rotate immediately
                            } else {
                                writeRecord(projectedResult.getProjectedRecord());
                                buffer.poll();
                                break;
                            }
                        }
                    case SHOULD_ROTATE:
                        updateRotationTimers(currentRecord);
                        closeTempFile();
                        nextState();
                    case TEMP_FILE_CLOSED:
                        appendToWAL();
                        nextState();
                    case WAL_APPENDED:
                        commitFile();
                        nextState();
                    case FILE_COMMITTED:
                        setState(State.WRITE_PARTITION_PAUSED);
                        break;
                    default:
                        log.error("{} is not a valid state to write record for topic partition {}.", state, topicPartition);
                }
            } catch (SchemaProjectorException | IllegalWorkerStateException | HiveMetaStoreException e) {
                throw new RuntimeException(e);
            } catch (ConnectException e) {
                log.error("Exception on topic partition {}: ", topicPartition, e);
                failureTime = time.milliseconds();
                setRetryTimeout(timeoutMs);
                break;
            }
        }
        if (buffer.isEmpty()) {
            try {
                switch (state) {
                    case WRITE_STARTED:
                        pause();
                        nextState();
                    case WRITE_PARTITION_PAUSED:
                        // committing files after waiting for rotateIntervalMs time but less than flush.size
                        // records available
                        if (recordCounter == 0 || !shouldRotateAndMaybeUpdateTimers(currentRecord, now)) {
                            break;
                        }

                        log.info(
                            "committing files after waiting for rotateIntervalMs time but less than "
                                + "flush.size records available."
                        );
                        nextState();
                    case SHOULD_ROTATE:
                        updateRotationTimers(currentRecord);
                        closeTempFile();
                        nextState();
                    case TEMP_FILE_CLOSED:
                        appendToWAL();
                        nextState();
                    case WAL_APPENDED:
                        commitFile();
                        nextState();
                    case FILE_COMMITTED:
                        break;
                    default:
                        log.error("{} is not a valid state to empty batch for topic partition {}.", state, topicPartition);
                }
            } catch (ConnectException e) {
                log.error("Exception on topic partition {}: ", topicPartition, e);
                failureTime = time.milliseconds();
                setRetryTimeout(timeoutMs);
                return;
            }

            resume();
            state = State.WRITE_STARTED;
        }
    }
    // CHECKSTYLE:OFF

    @Override
    public void close() throws ConnectException {
        log.debug("Closing TopicPartitionWriter {}", topicPartition);
        List<Exception> exceptions = new ArrayList<>();
        for (String encodedPartition : tempFiles.keySet()) {
            log.debug(
                "Discarding in progress tempfile {} for {} {}",
                tempFiles.get(encodedPartition),
                    topicPartition,
                encodedPartition
            );

            try {
                closeTempFile(encodedPartition);
            } catch (ConnectException e) {
                log.error(
                    "Error closing temp file {} for {} {} when closing TopicPartitionWriter:",
                    tempFiles.get(encodedPartition),
                        topicPartition,
                    encodedPartition,
                    e
                );
            }

            try {
                deleteTempFile(encodedPartition);
            } catch (ConnectException e) {
                log.error(
                    "Error deleting temp file {} for {} {} when closing TopicPartitionWriter:",
                    tempFiles.get(encodedPartition),
                        topicPartition,
                    encodedPartition,
                    e
                );
            }
        }

        writers.clear();

        try {
            wal.close();
        } catch (ConnectException e) {
            log.error("Error closing {}.", wal.getLogFile(), e);
            exceptions.add(e);
        }
        startOffsets.clear();
        endOffsets.clear();

        if (exceptions.size() != 0) {
            StringBuilder sb = new StringBuilder();
            for (Exception exception : exceptions) {
                sb.append(exception.getMessage());
                sb.append("\n");
            }
            throw new ConnectException("Error closing writer: " + sb.toString());
        }
    }

    @Override
    public void commit() {

    }

    @Override
    public Schema doChangeSchema(final Schema curSchema) {
        hiveMetaRestorer.repairHiveTable(curSchema);
        return curSchema;
    }

    @Override
    public void buffer(final SinkRecord sinkRecord) {
        log.trace("Buffering record with offset {}", sinkRecord.kafkaOffset());
        buffer.add(sinkRecord);
    }

    /**
     * HDFS Connector tracks offsets in filenames in HDFS (for Exactly Once Semantics) as the last record's offset that was written to the last file in HDFS. This method returns the next offset after
     * the last one in HDFS, useful for some APIs (like Kafka Consumer offset tracking).
     *
     * @return Next offset after the last offset written to HDFS, or -1 if no file has been committed yet
     */
    @Override
    public long offset() {
        return offset;
    }

    @Override
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    private void nextState() {
        state = state.next();
    }

    private void setState(final State state) {
        this.state = state;
    }

    private boolean shouldRotateAndMaybeUpdateTimers(final SinkRecord currentRecord, long now) {
        Long currentTimestamp = null;
        if (isWallclockBased) {
            currentTimestamp = now;
        } else if (currentRecord != null) {
            currentTimestamp = timestampExtractor.extract(currentRecord);
            lastRotate = lastRotate == null ? currentTimestamp : lastRotate;
        }

        boolean periodicRotation = rotateIntervalMs > 0
            && currentTimestamp != null
            && lastRotate != null
            && currentTimestamp - lastRotate >= rotateIntervalMs;
        boolean scheduledRotation = rotateScheduleIntervalMs > 0 && now >= nextScheduledRotate;
        boolean messageSizeRotation = recordCounter >= flushSize;

        log.trace(
            "Should apply periodic time-based rotation (rotateIntervalMs: '{}', lastRotate: "
                + "'{}', timestamp: '{}')? {}",
            rotateIntervalMs,
            lastRotate,
            currentTimestamp,
            periodicRotation
        );

        log.trace(
            "Should apply scheduled rotation: (rotateScheduleIntervalMs: '{}', nextScheduledRotate:"
                + " '{}', now: '{}')? {}",
            rotateScheduleIntervalMs,
            nextScheduledRotate,
            now,
            scheduledRotation
        );

        log.trace(
            "Should apply size-based rotation (count {} >= flush size {})? {}",
            recordCounter,
            flushSize,
            messageSizeRotation
        );

        return periodicRotation || scheduledRotation || messageSizeRotation;
    }

    /**
     * Read the offset of most recent record in HDFS. Attempt to read the offset from the WAL file and fall-back on a recursive search of filenames.
     */
    private void readOffset() {
        // Use the WAL file to attempt to extract the recent offsets
        FilePathOffset latestOffsetEntry = wal.extractLatestOffset();
        if (latestOffsetEntry != null) {
            long lastCommittedOffset = latestOffsetEntry.getOffset();
            log.trace("Last committed offset based on WAL: {}", lastCommittedOffset);
            offset = lastCommittedOffset + 1;
            log.trace("Next offset to read: {}", offset);
            return;
        }

        // Use the recursive filename scan approach
        log.debug("Could not use WAL approach for recovering offsets, "
            + "searching for latest offsets on HDFS.");
        Optional<FileStatus> fileStatus = fileOperator.findCommittedFileWithMaxVersionForTopicPartitionInTablePath(topicPartition);
        if (fileStatus.isPresent()) {
            long lastCommittedOffsetToHdfs = HdfsFileOperator.extractVersion(
                fileStatus.get().getPath().getName());
            log.trace("Last committed offset based on filenames: {}", lastCommittedOffsetToHdfs);
            // `offset` represents the next offset to read after the most recent commit
            offset = lastCommittedOffsetToHdfs + 1;
            log.trace("Next offset to read: {}", offset);
        }
    }

    private void pause() {
        context.pause(topicPartition);
    }

    private void resume() {
        context.resume(topicPartition);
    }

    @SuppressWarnings("checkstyle:IllegalCatch")
    private RecordWriter getWriter(
        final SinkRecord record,
        final String encodedPartition
    ) {
        if (writers.containsKey(encodedPartition)) {
            return writers.get(encodedPartition);
        }
        String tempFile = getTempFile(encodedPartition);

        final RecordWriter writer;
        writer = writerProvider.newRecordWriter(tempFile);

        writers.put(encodedPartition, writer);
        return writer;
    }

    private String getTempFile(final String encodedPartition) {
        String tempFile;
        if (tempFiles.containsKey(encodedPartition)) {
            tempFile = tempFiles.get(encodedPartition);
        } else {
            tempFile = fileOperator.createTempFileInTempTablePartitionPath(encodedPartition, extension);
            tempFiles.put(encodedPartition, tempFile);
        }
        return tempFile;
    }

    private void applyWAL() throws ConnectException {
        if (!recovered) {
            wal.apply();
        }
    }

    private void truncateWAL() throws ConnectException {
        wal.truncate();
    }

    private void resetOffsets() throws ConnectException {
        if (!recovered) {
            readOffset();
            // Note that we must *always* request that we seek to an offset here. Currently the
            // framework will still commit Kafka offsets even though we track our own (see KAFKA-3462),
            // which can result in accidentally using that offset if one was committed but no files
            // were rolled to their final location in HDFS (i.e. some data was accepted, written to a
            // tempfile, but then that tempfile was discarded). To protect against this, even if we
            // just want to start at offset 0 or reset to the earliest offset, we specify that
            // explicitly to forcibly override any committed offsets.
            if (offset > 0) {
                log.debug("Resetting offset for {} to {}", topicPartition, offset);
                context.offset(topicPartition, offset);
            } else {
                // The offset was not found, so rather than forcibly set the offset to 0 we let the
                // consumer decide where to start based upon standard consumer offsets (if available)
                // or the consumer's `auto.offset.reset` configuration
                log.debug("Resetting offset for {} based upon existing consumer group offsets or, if "
                        + "there are none, the consumer's 'auto.offset.reset' value.",
                        topicPartition);
            }
            recovered = true;
        }
    }

    private void writeRecord(final SinkRecord record) {
        if (offset == -1) {
            offset = record.kafkaOffset();
        }

        String encodedPartition = partitioner.encodePartition(record);
        RecordWriter writer = getWriter(record, encodedPartition);
        writer.write(record);

        if (!startOffsets.containsKey(encodedPartition)) {
            startOffsets.put(encodedPartition, record.kafkaOffset());
        }
        endOffsets.put(encodedPartition, record.kafkaOffset());
        hiveMetaRestorer.addPartitionIfAbsent(encodedPartition);
        recordCounter++;
    }

    private void closeTempFile(final String encodedPartition) {
        // Here we remove the writer first, and then if non-null attempt to close it.
        // This is the correct logic, because if `close()` throws an exception and fails, the task
        // will catch this an ultimately retry writing the records in that topic partition.
        // But to do so, we need to get a new `RecordWriter`, and `getWriter(...)` would only
        // do that if there is no existing writer in the `writers` map.
        // Plus, once a `writer.close()` method is called, per the `Closeable` contract we should
        // not use it again. Therefore, it's actually better to remove the writer before
        // trying to close it, even if the close attempt fails.
        RecordWriter writer = writers.remove(encodedPartition);
        if (writer != null) {
            writer.close();
        }
    }

    private void closeTempFile() {
        ConnectException connectException = null;
        for (String encodedPartition : tempFiles.keySet()) {
            // Close the file and propagate any errors
            try {
                closeTempFile(encodedPartition);
            } catch (ConnectException e) {
                // still want to close all of the other data writers
                connectException = e;
                log.error(
                    "Failed to close temporary file for partition {}. The connector will attempt to"
                        + " rewrite the temporary file.",
                    encodedPartition
                );
            }
        }

        if (connectException != null) {
            // at least one tmp file did not close properly therefore will try to recreate the tmp and
            // delete all buffered records + tmp files and start over because otherwise there will be
            // duplicates, since there is no way to reclaim the records in the tmp file.
            for (String encodedPartition : tempFiles.keySet()) {
                try {
                    deleteTempFile(encodedPartition);
                } catch (ConnectException e) {
                    log.error("Failed to delete tmp file {}", tempFiles.get(encodedPartition), e);
                }
                startOffsets.remove(encodedPartition);
                endOffsets.remove(encodedPartition);
                buffer.clear();
            }

            log.debug("Resetting offset for {} to {}", topicPartition, offset);
            context.offset(topicPartition, offset);

            recordCounter = 0;
            throw connectException;
        }
    }

    private void appendToWAL(final String encodedPartition) {
        String tempFile = tempFiles.get(encodedPartition);
        if (appended.contains(tempFile)) {
            return;
        }
        if (!startOffsets.containsKey(encodedPartition)) {
            return;
        }
        long startOffset = startOffsets.get(encodedPartition);
        long endOffset = endOffsets.get(encodedPartition);
        String committedFile = fileOperator.createCommittedFileInTablePartitionPath(
            encodedPartition,
                topicPartition,
            startOffset,
            endOffset,
            extension
        );
        wal.append(tempFile, committedFile);
        appended.add(tempFile);
    }

    private void appendToWAL() {
        beginAppend();
        for (String encodedPartition : tempFiles.keySet()) {
            appendToWAL(encodedPartition);
        }
        endAppend();
    }

    private void beginAppend() {
        if (!appended.contains(WAL.beginMarker)) {
            wal.append(WAL.beginMarker, "");
        }
    }

    private void endAppend() {
        if (!appended.contains(WAL.endMarker)) {
            wal.append(WAL.endMarker, "");
        }
    }

    private void commitFile() {
        log.debug("Committing files");
        appended.clear();

        // commit all files and get the latest committed offset
        long latestCommitted = tempFiles.keySet().stream()
            .mapToLong(this::commitFile)
            .max()
            .orElse(-1);
        if (latestCommitted > -1) {
            offset = latestCommitted + 1;
        }
    }

    private long commitFile(final String encodedPartition) {
        if (!startOffsets.containsKey(encodedPartition)) {
            return -1;
        }
        log.debug("Committing file for partition {}", encodedPartition);
        long startOffset = startOffsets.get(encodedPartition);
        long endOffset = endOffsets.get(encodedPartition);
        String tempFile = tempFiles.get(encodedPartition);
        String committedFile = fileOperator.createCommittedFileInTablePartitionPath(
            encodedPartition,
                topicPartition,
            startOffset,
            endOffset,
            extension
        );

        String tablePartitionPath = fileOperator.createTablePartitionPath(encodedPartition);
        if (!storage.exists(tablePartitionPath)) {
            storage.create(tablePartitionPath);
        }
        storage.commit(tempFile, committedFile);
        startOffsets.remove(encodedPartition);
        endOffsets.remove(encodedPartition);
        recordCounter = 0;
        log.info("Committed {} for {}", committedFile, topicPartition);

        return endOffset;
    }

    private void deleteTempFile(final String encodedPartition) {
        storage.delete(tempFiles.get(encodedPartition));
    }

    private void setRetryTimeout(long timeoutMs) {
        context.timeout(timeoutMs);
    }

    private enum State {
        RECOVERY_STARTED,
        RECOVERY_PARTITION_PAUSED,
        WAL_APPLIED,
        OFFSET_RESET,
        WAL_TRUNCATED,
        WRITE_STARTED,
        WRITE_PARTITION_PAUSED,
        SHOULD_ROTATE,
        TEMP_FILE_CLOSED,
        WAL_APPENDED,
        FILE_COMMITTED;

        private static State[] vals = values();

        public State next() {
            return vals[(this.ordinal() + 1) % vals.length];
        }
    }
}
