package cn.xdf.acdc.connect.core.sink;

import cn.xdf.acdc.connect.core.sink.filter.CachedFilterProvider;
import cn.xdf.acdc.connect.core.sink.filter.FilterProvider;
import cn.xdf.acdc.connect.core.sink.processor.CachedSinkProcessorProvider;
import cn.xdf.acdc.connect.core.sink.processor.ProcessorProvider;
import cn.xdf.acdc.connect.core.sink.processor.SinkProcessor;
import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;

/**
 * Abstract writer contains ACDC custom functions, including:
 * 1. One topic data transfer to multi-destinations according to user's configuration.
 *  (PS: our sink writer only transfer one upstream kafka topic considering latency and throughput)
 * 2. Supporting ACDC core data processes;
 * 3. Define writer's lifecycle and process logic.
 *
 * @param <T> client type
 */
@Slf4j
public abstract class AbstractWriter<T> implements Writer {

    private final SinkConfig sinkConfig;

    private final ProcessorProvider<SinkProcessor> processorProvider;

    private final FilterProvider filterProvider;

    public AbstractWriter(final SinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        processorProvider = new CachedSinkProcessorProvider(sinkConfig);
        filterProvider = new CachedFilterProvider(sinkConfig);
    }

    /**
     * Write records to destination.
     *
     * @param records records to be written
     * @throws ConnectException when need not retry
     * @throws RetriableException when we can retry to fix it
     */
    public void write(final Collection<SinkRecord> records) throws ConnectException, RetriableException {
        // Make sure the client is on work.
        // eg: retry jdbc connection if timeout
        T client = getClient();

        beforeBatchRecordsProcess(client, records);

        // Traverse the records and write them out.
        for (SinkRecord recordBefore : records) {

            // Core record processes according to the custom configuration.
            for (String destination : sinkConfig.getDestinations()) {
                if (filterProvider.getFilter(destination).filter(recordBefore)) {
                    SinkRecord record = processorProvider.getProcessor(destination).process(recordBefore);

                    doWrite(client, destination, record);
                }
            }
        }

        afterBatchRecordsProcess(client, records);
    }

    protected void afterBatchRecordsProcess(final T client, final Collection<SinkRecord> records) {
        // default keep empty.
    }

    protected void beforeBatchRecordsProcess(final T client, final Collection<SinkRecord> records) {
        // default keep empty.
    }

    protected SinkConfig sinkConfig() {
        return sinkConfig;
    }

    protected abstract void doWrite(T client, String destination, SinkRecord record);

    protected abstract T getClient() throws ConnectException;

    /**
     * Called when sink task's kafka consumer closing partitions.
     *
     * @param partitions partitions to close
     */
    public abstract void closePartitions(Collection<TopicPartition> partitions);
}
