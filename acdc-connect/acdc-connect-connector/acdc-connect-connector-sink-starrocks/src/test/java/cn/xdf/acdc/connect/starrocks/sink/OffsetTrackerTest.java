package cn.xdf.acdc.connect.starrocks.sink;

import cn.xdf.acdc.connect.starrocks.sink.util.ReflectionUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class OffsetTrackerTest {
    
    private static final String DATA_COLLECTION_KEY = "db1.tb1";
    
    @Test
    public void testMarkProcessed() throws NoSuchFieldException, IllegalAccessException {
        OffsetTracker offsetTracker = new OffsetTracker();
        final Map<String, Map<TopicPartition, Long>> markedOffsets = (Map<String, Map<TopicPartition, Long>>) ReflectionUtils.getLimitedAccessField(offsetTracker, "markedOffsets");
        
        SinkRecord sinkRecord1 = createSinkRecord("topic1", 1, 2L);
        SinkRecord sinkRecord2 = createSinkRecord("topic2", 2, 3L);
        
        offsetTracker.markProcessed(DATA_COLLECTION_KEY, sinkRecord1);
        offsetTracker.markProcessed(DATA_COLLECTION_KEY, sinkRecord2);
        
        Assert.assertEquals(generateExpectValue(sinkRecord1, sinkRecord2), markedOffsets);
    }
    
    private SinkRecord createSinkRecord(final String topic, final Integer partition, final Long offset) {
        return new SinkRecord(topic, partition, null, null, null, null, offset, null, null, null);
    }
    
    private Map<String, Map<TopicPartition, Long>> generateExpectValue(final SinkRecord... records) {
        Map<String, Map<TopicPartition, Long>> expectValue = new HashMap<>();
        
        Map<TopicPartition, Long> offset = new HashMap<>();
        
        for (SinkRecord record : records) {
            offset.put(new TopicPartition(record.topic(), record.kafkaPartition()), record.kafkaOffset());
            
        }
        
        expectValue.put(DATA_COLLECTION_KEY, offset);
        
        return expectValue;
    }
    
    @Test
    public void testFlush() throws NoSuchFieldException, IllegalAccessException {
        OffsetTracker offsetTracker = new OffsetTracker();
        final Map<String, Map<TopicPartition, Long>> flushedOffsets = (Map<String, Map<TopicPartition, Long>>) ReflectionUtils.getLimitedAccessField(offsetTracker, "flushedOffsets");
        
        SinkRecord sinkRecord1 = createSinkRecord("topic1", 1, 2L);
        SinkRecord sinkRecord2 = createSinkRecord("topic2", 2, 3L);
        
        offsetTracker.markProcessed(DATA_COLLECTION_KEY, sinkRecord1);
        offsetTracker.markProcessed(DATA_COLLECTION_KEY, sinkRecord2);
        
        offsetTracker.flush(DATA_COLLECTION_KEY);
        
        Assert.assertEquals(generateExpectValue(sinkRecord1, sinkRecord2), flushedOffsets);
    }
    
    @Test
    public void testFlushShouldDoNothingWhenNoProcessedRecordExists() throws NoSuchFieldException, IllegalAccessException {
        OffsetTracker offsetTracker = new OffsetTracker();
        offsetTracker.flush(DATA_COLLECTION_KEY);
        final Map<String, Map<TopicPartition, Long>> flushedOffsets = (Map<String, Map<TopicPartition, Long>>) ReflectionUtils.getLimitedAccessField(offsetTracker, "flushedOffsets");
        
        Assert.assertTrue(flushedOffsets.isEmpty());
    }
    
    @Test
    public void testCommit() throws NoSuchFieldException, IllegalAccessException {
        OffsetTracker offsetTracker = new OffsetTracker();
        final Map<String, Map<TopicPartition, Long>> committedOffsets = (Map<String, Map<TopicPartition, Long>>) ReflectionUtils.getLimitedAccessField(offsetTracker, "committedOffsets");
        
        SinkRecord sinkRecord1 = createSinkRecord("topic1", 1, 2L);
        SinkRecord sinkRecord2 = createSinkRecord("topic2", 2, 3L);
        
        offsetTracker.markProcessed(DATA_COLLECTION_KEY, sinkRecord1);
        offsetTracker.markProcessed(DATA_COLLECTION_KEY, sinkRecord2);
        offsetTracker.flush(DATA_COLLECTION_KEY);
        
        offsetTracker.commit(DATA_COLLECTION_KEY);
        
        Assert.assertEquals(generateExpectValue(sinkRecord1, sinkRecord2), committedOffsets);
    }
    
    @Test
    public void testCommitShouldDoNothingWhenNoFlushedRecordExists() throws NoSuchFieldException, IllegalAccessException {
        OffsetTracker offsetTracker = new OffsetTracker();
        offsetTracker.flush(DATA_COLLECTION_KEY);
        final Map<String, Map<TopicPartition, Long>> committedOffsets = (Map<String, Map<TopicPartition, Long>>) ReflectionUtils.getLimitedAccessField(offsetTracker, "committedOffsets");
        
        Assert.assertTrue(committedOffsets.isEmpty());
    }
    
    @Test
    public void testOffsets() {
        OffsetTracker offsetTracker = new OffsetTracker();
        SinkRecord sinkRecord1 = createSinkRecord("topic1", 1, 2L);
        SinkRecord sinkRecord2 = createSinkRecord("topic2", 2, 3L);
        
        offsetTracker.markProcessed(DATA_COLLECTION_KEY, sinkRecord1);
        offsetTracker.markProcessed(DATA_COLLECTION_KEY, sinkRecord2);
        offsetTracker.flush(DATA_COLLECTION_KEY);
        
        offsetTracker.commit(DATA_COLLECTION_KEY);
        
        Map<TopicPartition, OffsetAndMetadata> expectValue = new HashMap<>();
        expectValue.put(new TopicPartition(sinkRecord1.topic(), sinkRecord1.kafkaPartition()), new OffsetAndMetadata(sinkRecord1.kafkaOffset() + 1));
        expectValue.put(new TopicPartition(sinkRecord2.topic(), sinkRecord2.kafkaPartition()), new OffsetAndMetadata(sinkRecord2.kafkaOffset() + 1));
        
        Assert.assertEquals(expectValue, offsetTracker.offsets());
    }
    
    @Test
    public void testOffsetsShouldGetEmptyWhenNoFlushedRecordExists() {
        OffsetTracker offsetTracker = new OffsetTracker();
        
        Assert.assertTrue(offsetTracker.offsets().isEmpty());
    }
    
    @Test
    public void testClose() throws NoSuchFieldException, IllegalAccessException {
        OffsetTracker offsetTracker = new OffsetTracker();
        final Map<String, Map<TopicPartition, Long>> markedOffsets = (Map<String, Map<TopicPartition, Long>>) ReflectionUtils.getLimitedAccessField(offsetTracker, "markedOffsets");
        final Map<String, Map<TopicPartition, Long>> flushedOffsets = (Map<String, Map<TopicPartition, Long>>) ReflectionUtils.getLimitedAccessField(offsetTracker, "flushedOffsets");
        final Map<String, Map<TopicPartition, Long>> committedOffsets = (Map<String, Map<TopicPartition, Long>>) ReflectionUtils.getLimitedAccessField(offsetTracker, "committedOffsets");
        
        SinkRecord sinkRecord1 = createSinkRecord("topic1", 1, 2L);
        SinkRecord sinkRecord2 = createSinkRecord("topic2", 2, 3L);
        
        offsetTracker.markProcessed(DATA_COLLECTION_KEY, sinkRecord1);
        offsetTracker.markProcessed(DATA_COLLECTION_KEY, sinkRecord2);
        offsetTracker.flush(DATA_COLLECTION_KEY);
        
        offsetTracker.commit(DATA_COLLECTION_KEY);
        
        offsetTracker.close(null);
        Assert.assertTrue(markedOffsets.isEmpty());
        Assert.assertTrue(flushedOffsets.isEmpty());
        Assert.assertTrue(committedOffsets.isEmpty());
    }
}
