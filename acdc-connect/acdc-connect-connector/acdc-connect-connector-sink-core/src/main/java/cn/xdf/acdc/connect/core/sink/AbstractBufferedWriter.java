package cn.xdf.acdc.connect.core.sink;

import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * AbstractBufferedWriter uses a record buffer to do batch flush which need to be considered in sync data transfer
 * for high-throughput.
 *
 * @param <T> type of client
 */
@Slf4j
public abstract class AbstractBufferedWriter<T> extends AbstractWriter<T> {

    // thread unsafe
    private final Map<String, AbstractBufferedRecords> destinationBufferedRecordMapping = new HashMap<>();

    public AbstractBufferedWriter(final SinkConfig sinkConfig) {
        super(sinkConfig);
    }

    @Override
    protected void doWrite(final T client, final String destination, final SinkRecord record) {
        destinationBufferedRecordMapping.computeIfAbsent(destination, key -> getBufferedRecords(client, key)).add(record);
    }

    @Override
    protected void afterBatchRecordsProcess(final T client, final Collection<SinkRecord> records) {
        for (Map.Entry<String, AbstractBufferedRecords> entry : destinationBufferedRecordMapping.entrySet()) {
            String destination = entry.getKey();
            AbstractBufferedRecords buffer = entry.getValue();
            log.debug("Flushing records in {} for destination: {}", this.getClass().getName(), destination);
            buffer.flush();
            buffer.close();
        }
        commit(client);
        destinationBufferedRecordMapping.clear();
    }

    protected abstract AbstractBufferedRecords getBufferedRecords(T client, String destination) throws ConnectException;

    protected abstract void commit(T client) throws ConnectException;

}
