/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.xdf.acdc.connect.starrocks.sink.streamload.v2;

import cn.xdf.acdc.connect.starrocks.sink.OffsetTracker;
import cn.xdf.acdc.connect.starrocks.sink.SinkData;
import cn.xdf.acdc.connect.starrocks.sink.config.StarRocksSinkConfig;
import cn.xdf.acdc.connect.starrocks.sink.serialize.StarRocksCsvSerializer;
import cn.xdf.acdc.connect.starrocks.sink.serialize.StarRocksISerializer;
import cn.xdf.acdc.connect.starrocks.sink.streamload.LoadMetrics;
import cn.xdf.acdc.connect.starrocks.sink.streamload.StreamLoadManager;
import cn.xdf.acdc.connect.starrocks.sink.streamload.StreamLoadResponse;
import cn.xdf.acdc.connect.starrocks.sink.streamload.StreamLoadSnapshot;
import cn.xdf.acdc.connect.starrocks.sink.streamload.StreamLoadUtils;
import cn.xdf.acdc.connect.starrocks.sink.streamload.StreamLoader;
import cn.xdf.acdc.connect.starrocks.sink.streamload.TableRegion;
import cn.xdf.acdc.connect.starrocks.sink.streamload.TransactionStreamLoader;
import cn.xdf.acdc.connect.starrocks.sink.streamload.properties.StreamLoadProperties;
import cn.xdf.acdc.connect.starrocks.sink.streamload.properties.StreamLoadTableProperties;
import com.alibaba.fastjson.JSON;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static cn.xdf.acdc.connect.starrocks.sink.streamload.StreamLoadUtils.isStarRocksSupportTransactionLoad;

/**
 * An implementation of {@link StreamLoadManager}. In this manager, you can use normal stream load or
 * transaction stream load to load data to StarRocks. You can control which to use when constructing
 * the manager with parameter **properties**. If {@link StreamLoadProperties ()}
 * is true, transaction stream load will be used, otherwise the normal stream load. You can also control
 * how to commit the transaction stream load by parameter **enableAutoCommit**. If it's true, the
 * manager will commit the load automatically, otherwise you need to commit the load manually. Note that
 * this parameter should always be true for the normal stream load currently.
 * The usage for manual commit should like this
 * manager.write(); // write some recodes
 * manager.flush(); // ensure the data is flushed to StarRocks, and the transaction is prepared
 * manager.snapshot(); // take a snapshot the current transactions, mainly recording the labels
 * manager.commit(); // commit those snapshots
 */
public class TransactionStreamLoadManager implements StreamLoadManager, Serializable {
    
    private static final Logger LOG = LoggerFactory.getLogger(TransactionStreamLoadManager.class);
    
    private static final long serialVersionUID = 1L;
    
    enum State {
        ACTIVE, INACTIVE
    }
    
    private final StreamLoadProperties properties;
    
    private final StreamLoader streamLoader;
    
    // threshold to trigger flush
    private final long maxCacheBytes;
    
    // threshold to block write
    private final long maxWriteBlockCacheBytes;
    
    private final Map<String, TableRegion> regions = new HashMap<>();
    
    private final AtomicLong currentCacheBytes = new AtomicLong(0L);
    
    private final AtomicLong totalFlushRows = new AtomicLong(0L);
    
    private final AtomicLong numberTotalRows = new AtomicLong(0L);
    
    private final AtomicLong numberLoadRows = new AtomicLong(0L);
    
    private final FlushAndCommitStrategy flushAndCommitStrategy;
    
    private final long scanningFrequency;
    
    private Thread manager;
    
    private final Lock lock = new ReentrantLock();
    
    private final Condition writable = lock.newCondition();
    
    private final Condition flushable = lock.newCondition();
    
    private final AtomicReference<State> state = new AtomicReference<>(State.INACTIVE);
    
    private volatile Throwable e;
    
    private final Queue<TableRegion> flushQ = new LinkedList<>();
    
    /**
     * Whether write() has triggered a flush after currentCacheBytes > maxCacheBytes.
     * This flag is set true after the flush is triggered in writer(), and set false
     * after the flush completed in callback(). During this period, there is no need
     * to re-trigger a flush.
     */
    private transient AtomicBoolean writeTriggerFlush;
    
    private transient LoadMetrics loadMetrics;
    
    private final OffsetTracker offsetTracker;
    
    private final StarRocksISerializer serializer;
    
    private final StarRocksSinkConfig sinkConfig;
    
    public TransactionStreamLoadManager(
            final StarRocksSinkConfig sinkConfig,
            final OffsetTracker offsetTracker
    ) {
        this.sinkConfig = sinkConfig;
        this.properties = sinkConfig.getProperties();
        this.streamLoader = new TransactionStreamLoader();
        this.maxCacheBytes = properties.getMaxCacheBytes();
        this.maxWriteBlockCacheBytes = 2 * maxCacheBytes;
        this.scanningFrequency = properties.getScanningFrequency();
        this.flushAndCommitStrategy = new FlushAndCommitStrategy(properties, true);
        this.serializer = new StarRocksCsvSerializer(sinkConfig.getCsvColumnSeparator(), sinkConfig.getColumnsList());
        this.offsetTracker = offsetTracker;
        
        detectStarRocksFeature(sinkConfig);
    }
    
    public TransactionStreamLoadManager(
            final StarRocksSinkConfig sinkConfig,
            final OffsetTracker offsetTracker,
            final StreamLoader streamLoader,
            final StarRocksISerializer serializer
    ) {
        this.sinkConfig = sinkConfig;
        this.properties = sinkConfig.getProperties();
        this.streamLoader = streamLoader;
        this.maxCacheBytes = properties.getMaxCacheBytes();
        this.maxWriteBlockCacheBytes = 2 * maxCacheBytes;
        this.scanningFrequency = properties.getScanningFrequency();
        this.flushAndCommitStrategy = new FlushAndCommitStrategy(properties, true);
        this.serializer = serializer;
        this.offsetTracker = offsetTracker;
    }
    
    @Override
    public void init() {
        this.writeTriggerFlush = new AtomicBoolean(false);
        this.loadMetrics = new LoadMetrics();
        if (state.compareAndSet(State.INACTIVE, State.ACTIVE)) {
            this.manager = new Thread(() -> {
                long lastPrintTimestamp = -1;
                LOG.info("manager running, scanningFrequency : {}", scanningFrequency);
                while (true) {
                    lock.lock();
                    try {
                        flushable.await(scanningFrequency, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        break;
                    } finally {
                        lock.unlock();
                    }
                    
                    if (lastPrintTimestamp == -1 || System.currentTimeMillis() - lastPrintTimestamp > 10000) {
                        lastPrintTimestamp = System.currentTimeMillis();
                        LOG.debug("Audit information: {}, {}", loadMetrics, flushAndCommitStrategy);
                    }
                    for (TableRegion region : flushQ) {
                        region.getAndIncrementAge();
                        if (flushAndCommitStrategy.shouldCommit(region)) {
                            boolean success = ((TransactionTableRegion) region).commit();
                            if (success && Objects.isNull(e)) {
                                // commit offset
                                offsetTracker.commit(region.getUniqueKey());
                                region.resetAge();
                            }
                            LOG.debug("Commit region {} for normal, success: {}", region.getUniqueKey(), success);
                        }
                    }
                    
                    for (TableRegion region : flushAndCommitStrategy.selectFlushRegions(flushQ, currentCacheBytes.get())) {
                        boolean flush = region.flush();
                        LOG.debug("Trigger flush table region {} because of selection, region cache bytes: {}," +
                                " flush: {}", region.getUniqueKey(), region.getCacheBytes(), flush);
                    }
                }
            }, "StarRocks-Sink-Manager");
            manager.setDaemon(true);
            manager.start();
            manager.setUncaughtExceptionHandler((t, ee) -> {
                LOG.error("StarRocks-Sink-Manager error", ee);
                e = ee;
            });
            LOG.info("StarRocks-Sink-Manager start, streamLoader: {}",
                    streamLoader.getClass().getName());
            
            streamLoader.start(properties, this);
        }
    }
    
    @Override
    public void write(String database, String table, SinkData<byte[]> sinkData) {
        AssertNotException();
        
        TableRegion region = getCacheRegion(database, table);
        if (LOG.isTraceEnabled()) {
            LOG.trace("Write database {}, table {}, row {}",
                    database, table, sinkData.getData());
        }
        int bytes = region.write(sinkData);
        
        long cachedBytes = currentCacheBytes.addAndGet(bytes);
        if (cachedBytes >= maxWriteBlockCacheBytes) {
            long startTime = System.nanoTime();
            lock.lock();
            try {
                int idx = 0;
                while (currentCacheBytes.get() >= maxWriteBlockCacheBytes) {
                    AssertNotException();
                    LOG.info("Cache full, wait flush, currentBytes: {}, maxWriteBlockCacheBytes: {}",
                            currentCacheBytes.get(), maxWriteBlockCacheBytes);
                    flushable.signal();
                    writable.await(Math.min(++idx, 5), TimeUnit.SECONDS);
                }
            } catch (InterruptedException ex) {
                this.e = ex;
                throw new ConnectException(ex);
            } finally {
                lock.unlock();
            }
            loadMetrics.updateWriteBlock(1, System.nanoTime() - startTime);
        } else if (cachedBytes >= maxCacheBytes && writeTriggerFlush.compareAndSet(false, true)) {
            lock.lock();
            try {
                flushable.signal();
            } finally {
                lock.unlock();
            }
            loadMetrics.updateWriteTriggerFlush(1);
            LOG.info("Trigger flush, currentBytes: {}, maxCacheBytes: {}", cachedBytes, maxCacheBytes);
        }
    }
    
    @Override
    public void write(final Collection<SinkRecord> records) {
        AssertNotException();
        
        for (SinkRecord record : records) {
            byte[] data = serializer.serialize(record);
            this.write(sinkConfig.getDatabaseName(), sinkConfig.getTableName(), new SinkData<>(record, data));
        }
    }
    
    @Override
    public void callback(StreamLoadResponse response) {
        long cacheByteBeforeFlush = response.getFlushBytes() != null ? currentCacheBytes.getAndAdd(-response.getFlushBytes()) : currentCacheBytes.get();
        if (response.getFlushRows() != null) {
            totalFlushRows.addAndGet(response.getFlushRows());
        }
        writeTriggerFlush.set(false);
        
        LOG.info("Receive load response, cacheByteBeforeFlush: {}, currentCacheBytes: {}, totalFlushRows : {}",
                cacheByteBeforeFlush, currentCacheBytes.get(), totalFlushRows.get());
        
        lock.lock();
        try {
            writable.signal();
        } finally {
            lock.unlock();
        }
        
        if (response.getException() != null) {
            LOG.error("Stream load failed, body : " + JSON.toJSONString(response.getBody()), response.getException());
            this.e = response.getException();
        }
        
        if (response.getBody() != null) {
            if (response.getBody().getNumberTotalRows() != null) {
                numberTotalRows.addAndGet(response.getBody().getNumberTotalRows());
            }
            if (response.getBody().getNumberLoadedRows() != null) {
                numberLoadRows.addAndGet(response.getBody().getNumberLoadedRows());
            }
        }
        
        if (response.getException() != null) {
            this.loadMetrics.updateFailedLoad();
        } else {
            this.loadMetrics.updateSuccessLoad(response);
        }
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("{}", loadMetrics);
        }
    }
    
    @Override
    public void callback(Throwable e) {
        LOG.error("Stream load failed", e);
        this.e = e;
    }
    
    @Override
    public StreamLoadSnapshot snapshot() {
        StreamLoadSnapshot snapshot = StreamLoadSnapshot.snapshot(regions.values());
        for (TableRegion region : regions.values()) {
            region.setLabel(null);
        }
        return snapshot;
    }
    
    @Override
    public boolean prepare(StreamLoadSnapshot snapshot) {
        return streamLoader.prepare(snapshot);
    }
    
    @Override
    public boolean commit(StreamLoadSnapshot snapshot) {
        return streamLoader.commit(snapshot);
    }
    
    @Override
    public boolean abort(StreamLoadSnapshot snapshot) {
        return streamLoader.rollback(snapshot);
    }
    
    @Override
    public boolean abort() {
        return abort(snapshot());
    }
    
    @Override
    public void close() {
        if (state.compareAndSet(State.ACTIVE, State.INACTIVE)) {
            LOG.info("StreamLoadManagerV2 close, loadMetrics: {}, flushAndCommit: {}",
                    loadMetrics, flushAndCommitStrategy);
            manager.interrupt();
            streamLoader.close();
        }
    }
    
    private void AssertNotException() {
        if (e != null) {
            LOG.error("catch exception, wait rollback ", e);
            streamLoader.rollback(snapshot());
            close();
            throw new ConnectException(e);
        }
    }
    
    protected TableRegion getCacheRegion(String database, String table) {
        String uniqueKey = StreamLoadUtils.getTableUniqueKey(database, table);
        TableRegion region = regions.get(uniqueKey);
        if (region == null) {
            synchronized (regions) {
                region = regions.get(uniqueKey);
                if (region == null) {
                    StreamLoadTableProperties tableProperties = properties.getTableProperties(uniqueKey);
                    region = new TransactionTableRegion(uniqueKey, database, table, this, tableProperties, streamLoader, offsetTracker);
                    regions.put(uniqueKey, region);
                    flushQ.offer(region);
                }
            }
        }
        return region;
    }
    
    private void detectStarRocksFeature(StarRocksSinkConfig starRocksSinkConfig) {
        try {
            boolean supportTransactionLoad = isStarRocksSupportTransactionLoad(
                    starRocksSinkConfig.getLoadUrlList(), starRocksSinkConfig.getConnectTimeout(), starRocksSinkConfig.getUsername(), starRocksSinkConfig.getPassword());
            
            if (supportTransactionLoad) {
                LOG.info("StarRocks supports transaction load");
            } else {
                throw new ConnectException("StarRocks does not support transaction load");
            }
        } catch (Exception e) {
            throw new ConnectException("Can't decide whether StarRocks supports transaction load, and enable it by default.");
        }
    }
}
