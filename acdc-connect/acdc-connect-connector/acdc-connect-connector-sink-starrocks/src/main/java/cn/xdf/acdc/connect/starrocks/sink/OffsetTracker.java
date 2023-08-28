package cn.xdf.acdc.connect.starrocks.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class OffsetTracker {
    
    private final Map<String, Map<TopicPartition, Long>> markedOffsets = new ConcurrentHashMap<>();
    
    private final Map<String, Map<TopicPartition, Long>> flushedOffsets = new ConcurrentHashMap<>();
    
    private final Map<String, Map<TopicPartition, Long>> committedOffsets = new ConcurrentHashMap<>();
    
    /**
     * Mark processed.
     *
     * @param dataCollectionKey data collection key
     * @param sinkRecord sink record
     */
    public void markProcessed(
            final String dataCollectionKey,
            final SinkRecord sinkRecord
    ) {
        TopicPartition tp = new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition());
        Long offset = sinkRecord.kafkaOffset();
        
        Map<TopicPartition, Long> marked = markedOffsets
                .computeIfAbsent(dataCollectionKey, key -> new ConcurrentHashMap<>());
        
        marked.put(tp, offset);
    }
    
    /**
     * Gets the offset that can be committed, this method is called during the preCommit life cycle of sink task.
     *
     * @return the offset that can be committed for each topic partition
     */
    public Map<TopicPartition, OffsetAndMetadata> offsets() {
        Map<TopicPartition, OffsetAndMetadata> offsetMetas = new HashMap<>();
        
        for (Map.Entry<String, Map<TopicPartition, Long>> each : committedOffsets.entrySet()) {
            Map<TopicPartition, Long> dataCollectionOffsets = each.getValue();
            
            if (Objects.isNull(dataCollectionOffsets) || dataCollectionOffsets.isEmpty()) {
                continue;
            }
            
            for (Map.Entry<TopicPartition, Long> entry : dataCollectionOffsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                Long offset = entry.getValue();
                
                offsetMetas.put(new TopicPartition(tp.topic(), tp.partition()), new OffsetAndMetadata(offset + 1));
            }
        }
        
        return offsetMetas;
    }
    
    /**
     * This method is called when records that have been written to the data collection are flushed.
     *
     * @param dataCollectionKey data collection key
     */
    public void flush(final String dataCollectionKey) {
        
        Map<TopicPartition, Long> marked = markedOffsets.get(dataCollectionKey);
        
        if (Objects.isNull(marked) || marked.isEmpty()) {
            return;
        }
        
        Map<TopicPartition, Long> flushed = flushedOffsets
                .computeIfAbsent(dataCollectionKey, key -> new ConcurrentHashMap<>());
        
        for (Map.Entry<TopicPartition, Long> each : marked.entrySet()) {
            TopicPartition tp = each.getKey();
            Long offset = each.getValue();
            
            flushed.put(new TopicPartition(tp.topic(), tp.partition()), offset);
        }
    }
    
    /**
     * This method is called when records that have been written to the data collection are committed.
     *
     * @param dataCollectionKey data collection key
     */
    public void commit(final String dataCollectionKey) {
        Map<TopicPartition, Long> flushed = flushedOffsets.get(dataCollectionKey);
        
        if (Objects.isNull(flushed) || flushed.isEmpty()) {
            return;
        }
        
        Map<TopicPartition, Long> committed = committedOffsets
                .computeIfAbsent(dataCollectionKey, key -> new ConcurrentHashMap<>());
        
        for (Map.Entry<TopicPartition, Long> each : flushed.entrySet()) {
            TopicPartition tp = each.getKey();
            Long offset = each.getValue();
            
            committed.put(new TopicPartition(tp.topic(), tp.partition()), offset);
        }
    }
    
    /**
     * This method is called during the close life cycle of sink task.
     *
     * @param topicPartitions topic partitions
     */
    public void close(final Collection<TopicPartition> topicPartitions) {
        markedOffsets.clear();
        flushedOffsets.clear();
        committedOffsets.clear();
    }
}
