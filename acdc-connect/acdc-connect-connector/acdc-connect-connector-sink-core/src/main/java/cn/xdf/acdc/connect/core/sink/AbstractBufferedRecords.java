package cn.xdf.acdc.connect.core.sink;

import cn.xdf.acdc.connect.core.util.RecordValidator;
import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Slf4j
public abstract class AbstractBufferedRecords {

    private final SinkConfig config;

    private List<SinkRecord> records = new ArrayList<>();

    private Schema keySchema;

    private Schema valueSchema;

    private RecordValidator recordValidator;

    private boolean deletesInBatch;

    public AbstractBufferedRecords(final SinkConfig config) {
        this.config = config;
        this.recordValidator = RecordValidator.create(config);
    }

    /**
     * Add the record to the buffer.
     *
     * @param record record to be added
     * @return flushed records
     * @throws ConnectException normal connect exception
     * @throws RetriableException retriable connect exception
     */
    public List<SinkRecord> add(final SinkRecord record) throws ConnectException, RetriableException {
        recordValidator.validate(record);

        final List<SinkRecord> flushed = new ArrayList<>();

        boolean schemaChanged = false;
        if (!Objects.equals(keySchema, record.keySchema())) {
            keySchema = record.keySchema();
            schemaChanged = true;
        }
        if (Objects.isNull(record.valueSchema())) {
            // For deletes, value and optionally value schema come in as null.
            // We don't want to treat this as a schema change if key schemas is the same
            // otherwise we flush unnecessarily.
            if (config.isDeleteEnabled()) {
                deletesInBatch = true;
            }
        } else if (Objects.equals(valueSchema, record.valueSchema())) {
            if (config.isDeleteEnabled() && deletesInBatch) {
                // flush so an insert after a delete of same record isn't lost
                flushed.addAll(flush());
            }
        } else {
            // value schema is not null and has changed. This is a real schema change.
            valueSchema = record.valueSchema();
            schemaChanged = true;
        }

        if (schemaChanged) {
            // Each batch needs to have the same schemas, so get the buffered records out
            flushed.addAll(flush());
            initMetadata(record);
        }

        // set deletesInBatch if schema value is not null
        if (Objects.isNull(record.value()) && config.isDeleteEnabled()) {
            deletesInBatch = true;
        }

        records.add(record);

        if (records.size() >= config.getBatchSize()) {
            flushed.addAll(flush());
        }
        return flushed;
    }

    /**
     * Flush records in the buffer.
     *
     * @return flushed records
     * @throws ConnectException normal connect exception
     * @throws RetriableException retriable connect exception
     */
    public List<SinkRecord> flush() throws ConnectException, RetriableException {
        if (records.isEmpty()) {
            log.debug("Records is empty");
            return new ArrayList<>();
        }

        doFlush(records);

        final List<SinkRecord> flushedRecords = records;
        records = new ArrayList<>();
        deletesInBatch = false;
        return flushedRecords;
    }

    protected abstract void initMetadata(SinkRecord record) throws ConnectException, RetriableException;

    protected abstract void doFlush(List<SinkRecord> records) throws ConnectException, RetriableException;

    protected abstract void close() throws RetriableException;

}
