package cn.xdf.acdc.connect.starrocks.sink.streamload.v2;

import cn.xdf.acdc.connect.starrocks.sink.OffsetTracker;
import cn.xdf.acdc.connect.starrocks.sink.config.StarRocksSinkConfig;
import cn.xdf.acdc.connect.starrocks.sink.serialize.StarRocksISerializer;
import cn.xdf.acdc.connect.starrocks.sink.streamload.StreamLoader;
import cn.xdf.acdc.connect.starrocks.sink.streamload.TableRegion;
import cn.xdf.acdc.connect.starrocks.sink.streamload.v2.MockStreamLoader.MockMode;
import cn.xdf.acdc.connect.starrocks.sink.util.ConfigUtil;
import cn.xdf.acdc.connect.starrocks.sink.util.ReflectionUtils;
import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.locks.LockSupport;

public class TransactionStreamLoadManagerTest {
    
    private static final int MB = 1024 * 1024;
    
    @Test
    public void testInit() {
        StarRocksSinkConfig sinkConfig = new StarRocksSinkConfig(ConfigUtil.getSinkConfig());
        StarRocksISerializer serializer = Mockito.mock(StarRocksISerializer.class);
        OffsetTracker offsetTracker = new OffsetTracker();
        StreamLoader streamLoader = new MockStreamLoader();
        TransactionStreamLoadManager manager = new TransactionStreamLoadManager(
                sinkConfig,
                offsetTracker,
                streamLoader,
                serializer
        );
        
        manager.init();
    }
    
    @Test
    public void testWrite() {
        StreamLoader streamLoader = new MockStreamLoader();
        StarRocksISerializer serializer = Mockito.mock(StarRocksISerializer.class);
        StarRocksSinkConfig sinkConfig = new StarRocksSinkConfig(ConfigUtil.getSinkConfig());
        OffsetTracker offsetTracker = new OffsetTracker();
        TransactionStreamLoadManager manager = new TransactionStreamLoadManager(
                sinkConfig,
                offsetTracker,
                streamLoader,
                serializer
        );
        
        Mockito.when(serializer.serialize(Mockito.any()))
                .thenReturn(new byte[MB * 3]);
        
        manager.init();
        SinkRecord sinkRecord = createSinkRecord("test", 1, 11L);
        manager.write(Lists.newArrayList(sinkRecord));
        while (offsetTracker.offsets().isEmpty()) {
            LockSupport.parkNanos(50);
        }
        
        Map<TopicPartition, OffsetAndMetadata> expectValue = new HashMap<>();
        expectValue.put(
                new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition()),
                new OffsetAndMetadata(sinkRecord.kafkaOffset() + 1)
        );
        
        Assert.assertEquals(
                expectValue,
                offsetTracker.offsets()
        );
    }
    
    @Test(expected = ConnectException.class)
    public void testWriteShouldThrowException() throws NoSuchFieldException, IllegalAccessException {
        StreamLoader streamLoader = new MockStreamLoader(MockMode.STREAM_LOAD_FAILURE);
        StarRocksISerializer serializer = Mockito.mock(StarRocksISerializer.class);
        StarRocksSinkConfig sinkConfig = new StarRocksSinkConfig(ConfigUtil.getSinkConfig());
        OffsetTracker offsetTracker = new OffsetTracker();
        TransactionStreamLoadManager manager = new TransactionStreamLoadManager(
                sinkConfig,
                offsetTracker,
                streamLoader,
                serializer
        );
        
        manager.init();
        
        Mockito.when(serializer.serialize(Mockito.any()))
                .thenReturn(new byte[MB * 3]);
        
        SinkRecord sinkRecord = createSinkRecord("test", 1, 11L);
        
        while (true) {
            Throwable e = (Throwable) ReflectionUtils.getLimitedAccessField(manager, "e");
            if (Objects.nonNull(e))
                LockSupport.parkNanos(50);
            
            manager.write(Lists.newArrayList(sinkRecord));
        }
    }
    
    @Test
    public void testWriteShouldNotCommitOffsetWhenStreamLoadFailure() throws NoSuchFieldException, IllegalAccessException {
        StreamLoader streamLoader = new MockStreamLoader(MockMode.STREAM_LOAD_FAILURE);
        StarRocksISerializer serializer = Mockito.mock(StarRocksISerializer.class);
        StarRocksSinkConfig sinkConfig = new StarRocksSinkConfig(ConfigUtil.getSinkConfig());
        OffsetTracker offsetTracker = new OffsetTracker();
        TransactionStreamLoadManager manager = new TransactionStreamLoadManager(
                sinkConfig,
                offsetTracker,
                streamLoader,
                serializer
        );
        
        manager.init();
        
        Mockito.when(serializer.serialize(Mockito.any()))
                .thenReturn(new byte[MB * 3]);
        
        SinkRecord sinkRecord = createSinkRecord("test", 1, 11L);
        manager.write(Lists.newArrayList(sinkRecord));
        
        while (true) {
            Queue<TableRegion> flushQ = (Queue<TableRegion>) ReflectionUtils.getLimitedAccessField(manager, "flushQ");
            FlushAndCommitStrategy flushAndCommitStrategy = (FlushAndCommitStrategy) ReflectionUtils.getLimitedAccessField(manager, "flushAndCommitStrategy");
            TableRegion tableRegion = flushQ.peek();
            if (Objects.isNull(tableRegion) || !flushAndCommitStrategy.shouldCommit(tableRegion)) {
                LockSupport.parkNanos(50);
                continue;
            }
            
            break;
        }
        
        Assert.assertTrue(offsetTracker.offsets().isEmpty());
    }
    
    @Test(expected = ConnectException.class)
    public void testSinkTaskPollingCheckShouldThrowExceptionWhenAlreadyLoadFail() {
        StreamLoader streamLoader = new MockStreamLoader(MockMode.STREAM_LOAD_FAILURE);
        StarRocksISerializer serializer = Mockito.mock(StarRocksISerializer.class);
        StarRocksSinkConfig sinkConfig = new StarRocksSinkConfig(ConfigUtil.getSinkConfig());
        OffsetTracker offsetTracker = new OffsetTracker();
        TransactionStreamLoadManager manager = new TransactionStreamLoadManager(
                sinkConfig,
                offsetTracker,
                streamLoader,
                serializer
        );
        
        Mockito.when(serializer.serialize(Mockito.any()))
                .thenReturn(new byte[MB * 3]);
        manager.callback(new RuntimeException("test"));
        manager.write(null);
    }
    
    @Test
    public void testSinkTaskPollingCheckShouldDoNothingWhenRecordsIsEmpty() {
        StreamLoader streamLoader = new MockStreamLoader(MockMode.STREAM_LOAD_FAILURE);
        StarRocksISerializer serializer = Mockito.mock(StarRocksISerializer.class);
        StarRocksSinkConfig sinkConfig = new StarRocksSinkConfig(ConfigUtil.getSinkConfig());
        OffsetTracker offsetTracker = new OffsetTracker();
        TransactionStreamLoadManager manager = new TransactionStreamLoadManager(
                sinkConfig,
                offsetTracker,
                streamLoader,
                serializer
        );
        
        Mockito.when(serializer.serialize(Mockito.any()))
                .thenReturn(new byte[MB * 3]);
        
        manager.write(new ArrayList<>());
        
        Mockito.verify(serializer, Mockito.times(0)).serialize(Mockito.any());
    }
    
    private SinkRecord createSinkRecord(
            final String topic,
            final Integer partition,
            final Long offset
    ) {
        return new SinkRecord(
                topic,
                partition,
                null,
                null,
                null,
                null,
                offset,
                null,
                null,
                null
        );
    }
}
