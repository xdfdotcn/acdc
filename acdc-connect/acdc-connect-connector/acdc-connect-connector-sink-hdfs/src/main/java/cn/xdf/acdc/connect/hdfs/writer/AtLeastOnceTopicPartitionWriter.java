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

import cn.xdf.acdc.connect.hdfs.HdfsSinkConstants;
import cn.xdf.acdc.connect.hdfs.format.ProjectedResult;
import cn.xdf.acdc.connect.hdfs.format.RecordWriter;
import cn.xdf.acdc.connect.hdfs.format.RecordWriterProvider;
import cn.xdf.acdc.connect.hdfs.format.SchemaReader;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import cn.xdf.acdc.connect.hdfs.rotation.RotationPolicy;
import cn.xdf.acdc.connect.hdfs.storage.HdfsFileOperator;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

/**
 *At Least Once writer.
 */
@Slf4j
public class AtLeastOnceTopicPartitionWriter implements TopicPartitionWriter {

    private final TopicPartition tp;

    private final RecordWriterProvider writerProvider;

    private final Partitioner partitioner;

    private final Queue<SinkRecord> buffer;

    private final Map<String, RecordWriter> encodePartitionWriters;

    private final RotationPolicy rotationPolicy;

    private final SchemaReader schemaReader;

    private final HdfsFileOperator fileOperator;

    private final HiveMetaRestorer hiveMetaRestorer;

    private final StoreConfig storeConfig;

    private final String table;

    private long currentOffset = -1;

    private long writtenRecordCount;

    public AtLeastOnceTopicPartitionWriter(
        final SinkTaskContext sinkTaskContext,
        final TopicPartition tp,
        final StoreContext storeContext
    ) {
        // component
        this.tp = tp;
        this.storeConfig = storeContext.getStoreConfig();
        this.fileOperator = storeContext.getFileOperator();
        this.writerProvider = storeContext.getRecordWriterProvider();
        this.partitioner = storeContext.getPartitioner();
        this.schemaReader = storeContext.getSchemaReader();

        //initialize
        this.buffer = new LinkedList<>();
        this.encodePartitionWriters = new HashMap<>();
        this.rotationPolicy = storeContext.getRotationPolicy();
        this.hiveMetaRestorer = storeContext.getHiveMetaRestorer();
        this.table = new StringBuilder()
            .append(storeConfig.database())
            .append(HdfsSinkConstants.DB_SEPARATOR)
            .append(storeConfig.table())
            .toString();
    }

    @Override
    public void buffer(final SinkRecord sinkRecord) {
        if (log.isDebugEnabled()) {
            log.debug("Buffering record, table: {}, tp: {}, key: {}, offset: {}, value: {}",
                table, tp, sinkRecord.kafkaOffset(), sinkRecord.key(), sinkRecord.value()
            );
        }

        buffer.add(sinkRecord);
    }

    @Override
    public void write() {
        if (buffer.isEmpty()) {
            log.warn("Buffer record is empty, table: {}, tp: {}, buffer size {}",
                table, tp, buffer.size());
            return;
        }
        long start = System.currentTimeMillis();
        while (!buffer.isEmpty()) {
            writeRecord();
        }

        log.info("Write Success, table: {}, tp: {}, number of processing record: {}, cost: {}",
            table, tp, getWrittenRecordCount(), System.currentTimeMillis() - start
        );
    }

    private void writeRecord() {
        SinkRecord sinkRecord = buffer.poll();
        String encodePartition = partitioner.encodePartition(sinkRecord);
        RecordWriter writer = getRecordWriter(encodePartition);
        ProjectedResult projectedResult = schemaReader.projectRecord(tp, sinkRecord);

        if (projectedResult.isNeedChangeSchema()) {
            log.info("Schema change should be alert table, {}, table: newest schema: {}, projected record: {}",
                table, projectedResult.getCurrentSchema().fields(), projectedResult.getProjectedRecord()
            );
            doChangeSchema(projectedResult.getCurrentSchema());
        }

        writer.write(projectedResult.getProjectedRecord());

        if (log.isDebugEnabled()) {
            log.debug("Write record success, "
                    + "table: {}, "
                    + "tp: {}, "
                    + "encodePartition: {}, "
                    + "write filename: {}, "
                    + "current file size: {}, "
                    + "current schema: {}, "
                    + "projected record: {}",
                table, tp, encodePartition, writer.fileName(), writer.fileSize(),
                projectedResult.getCurrentSchema(), projectedResult.getProjectedRecord());
        }

        recordOffset(sinkRecord);
        increasingWrittenRecordCount();
        this.hiveMetaRestorer.addPartitionIfAbsent(encodePartition);
    }

    private void increasingWrittenRecordCount() {
        this.writtenRecordCount++;
    }

    private void clearWrittenRecordCount() {
        this.writtenRecordCount = 0;
    }

    private long getWrittenRecordCount() {
        return this.writtenRecordCount;
    }

    private RecordWriter getRecordWriter(final String encodePartition) {
        RecordWriter recordWriter;
        if (encodePartitionWriters.containsKey(encodePartition)) {
            recordWriter = encodePartitionWriters.get(encodePartition);
        } else {
            String commitFile = loadCommitFile(encodePartition);
            recordWriter = writerProvider.newRecordWriter(commitFile);
            encodePartitionWriters.put(encodePartition, recordWriter);
        }
        if (rotationPolicy.shouldBeRotateFile(recordWriter)) {
            closeRecordWriter(encodePartition);
            String commitFile = fileOperator.createCommitFileByRotation(encodePartition, tp, writerProvider.getExtension());
            recordWriter = writerProvider.newRecordWriter(commitFile);
            encodePartitionWriters.put(encodePartition, recordWriter);
        }
        return encodePartitionWriters.get(encodePartition);
    }

    private String loadCommitFile(final String encodePartition) {
        Optional<FileStatus> maxFile = fileOperator.findMaxVerFileByPartitionAndTp(tp, encodePartition);
        if (maxFile.isPresent()) {
            return maxFile.get().getPath().toString();
        } else {
            return fileOperator.createCommitFileByRotation(encodePartition,
                tp, writerProvider.getExtension());
        }
    }

    private void closeRecordWriter(final String encodePartition) {
        RecordWriter recordWriter = encodePartitionWriters.remove(encodePartition);
        Preconditions.checkNotNull(recordWriter, "not exist writer");
        log.info("Trigger file rotation,close the current file, "
                + "table: {}, "
                + "tp: {}, "
                + "encodePartition: {}, "
                + "filename: {}, "
                + "file size: {}, ",
            table, tp, encodePartition, recordWriter.fileName(), recordWriter.fileSize()
        );
        recordWriter.close();
    }

    private void closeRecordWriter() {
        for (RecordWriter recordWriter : encodePartitionWriters.values()) {
            recordWriter.close();
        }
        encodePartitionWriters.clear();
    }

    @Override
    public long offset() {
        return (this.currentOffset == -1L) ? -1L : (this.currentOffset + 1L);
    }

    @Override
    public TopicPartition topicPartition() {
        return tp;
    }

    @Override
    public boolean recover() {
        return true;
    }

    @Override
    public void close() {
        log.info("Will close all writer, tp: {}", tp);
        closeRecordWriter();
        clearWrittenRecordCount();
        rotationPolicy.reset();
    }

    @Override
    public void commit() {
        log.info("Will commit all writer, tp: {}", tp);
        if (buffer.isEmpty()) {
            closeRecordWriter();
            clearWrittenRecordCount();
        } else {
            String msg = String.format(
                "Error state for commit, buffer is not empty, buffer size is: ",
                buffer.size());
            new ConnectException(msg);
        }
    }

    @Override
    public Schema doChangeSchema(final Schema curSchema) {
        hiveMetaRestorer.repairHiveTable(curSchema);
        return curSchema;
    }

    private void recordOffset(final SinkRecord sinkRecord) {
        long offset = sinkRecord.kafkaOffset();
        this.currentOffset = offset;
    }

    /**
     * Get all record writers ,for test .
     * @return all record writer map
     */
    public Map<String, RecordWriter> getEncodePartitionWriters() {
        return encodePartitionWriters;
    }
}
