package cn.xdf.acdc.connect.starrocks.sink;

import cn.xdf.acdc.connect.starrocks.sink.config.StarRocksSinkConfig;
import cn.xdf.acdc.connect.starrocks.sink.streamload.StreamLoadManager;
import cn.xdf.acdc.connect.starrocks.sink.streamload.v2.TransactionStreamLoadManager;
import cn.xdf.acdc.connect.starrocks.util.Version;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

@Slf4j
public class StarRocksSinkTask extends SinkTask {
    
    private StarRocksSinkConfig starRocksSinkConfig;
    
    private StreamLoadManager streamLoadManager;
    
    private OffsetTracker offsetTracker;
    
    public StarRocksSinkTask() {
    }
    
    public StarRocksSinkTask(
            final StarRocksSinkConfig starRocksSinkConfig,
            final OffsetTracker offsetTracker,
            final StreamLoadManager streamLoadManager
    ) {
        this.starRocksSinkConfig = starRocksSinkConfig;
        this.offsetTracker = offsetTracker;
        this.streamLoadManager = streamLoadManager;
    }
    
    @Override
    public void start(final Map<String, String> props) {
        log.info("Starting starRocks sink task...");
        this.starRocksSinkConfig = new StarRocksSinkConfig(props);
        this.offsetTracker = new OffsetTracker();
        this.streamLoadManager = new TransactionStreamLoadManager(starRocksSinkConfig, offsetTracker);
        
        streamLoadManager.init();
    }
    
    @SneakyThrows
    @Override
    public void put(final Collection<SinkRecord> records) {
        streamLoadManager.write(records);
    }
    
    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        return offsetTracker.offsets();
    }
    
    @Override
    public void close(final Collection<TopicPartition> partitions) {
        log.info("Closing starRocks sink task...");
        offsetTracker.close(partitions);
    }
    
    @Override
    public void stop() {
        log.info("Stopping starRocks sink task...");
        streamLoadManager.abort();
        streamLoadManager.close();
    }
    
    @Override
    public String version() {
        return Version.getVersion();
    }
    
    @Override
    public void open(final Collection<TopicPartition> partitions) {
        log.info("Opening starRocks sink task...");
        super.open(partitions);
    }
}
