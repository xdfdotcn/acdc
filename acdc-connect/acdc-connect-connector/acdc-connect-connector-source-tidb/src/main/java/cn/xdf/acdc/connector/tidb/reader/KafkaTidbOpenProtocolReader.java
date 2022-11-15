package cn.xdf.acdc.connector.tidb.reader;

import cn.xdf.acdc.connector.tidb.ticdc.parser.TicdcOpenProtocolParser;
import cn.xdf.acdc.connector.tidb.util.DelayStrategy;
import com.google.common.collect.Lists;
import com.pingcap.ticdc.cdc.KafkaMessage;
import io.debezium.config.Configuration;

import cn.xdf.acdc.connector.tidb.source.TidbConnectorConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class KafkaTidbOpenProtocolReader implements TidbDataReader {

    private static final String DEFAULT_DESERIALIZER = "org.apache.kafka.common.serialization.ByteArrayDeserializer";

    private static final String DEFAULT_AUTO_OFFSET_RESET_CONFIG = "earliest";

    private static final String DEFAULT_ENABLE_AUTO_COMMIT_CONFIG = "false";

    private static final long DEFAULT_CONSUMER_POLL_DURATION = 100L;

    private static final int MAX_RETRY_TIMES = 3;

    private static final int RETRY_INTERVAL = 3_000;

    private static final int RUNNER_STOP_TIMEOUT = 20_000;

    private static final Object NULL_CONSUMER_PLACEHOLDER = new Object();

    private final Properties consumerProperties = new Properties();

    private final String subscribeTopic;

    private final AtomicInteger totalRunnerCount;

    private final AtomicInteger aliveRunnerCount = new AtomicInteger(0);

    private final List<EventListener> eventListeners = new CopyOnWriteArrayList<>();

    private final List<RunnerLifecycleListener> runnerLifecycleListeners = new CopyOnWriteArrayList<>();

    private final Map<Thread, Object> threadsHolder = new ConcurrentHashMap<>();

    private volatile ReaderStatus status;

    private final KafkaReaderOffsetManager offsetManager = new KafkaReaderOffsetManager();

    private final DelayStrategy retryIntervalDelayStrategy = DelayStrategy.constant(RETRY_INTERVAL);

    public KafkaTidbOpenProtocolReader(final Configuration config) {
        this.subscribeTopic = config.getString(TidbConnectorConfig.SOURCE_KAFKA_TOPIC);
        this.totalRunnerCount = new AtomicInteger(config.getInteger(TidbConnectorConfig.SOURCE_KAFKA_READER_THREAD_NUMBER));
        initConsumerProperties(config);
        registerRunnerLifecycleListener(defaultRunnerLifecycleListener());
        status = ReaderStatus.INITTED;
    }

    private RunnerLifecycleListener defaultRunnerLifecycleListener() {
        return new RunnerLifecycleListener() {
            @Override
            public void onStart() {
                aliveRunnerCount.incrementAndGet();
            }

            @Override
            public void triggerStop() {
                throw new WakeupException();
            }

            @Override
            public void onEnd() {
                aliveRunnerCount.decrementAndGet();
            }
        };
    }

    private void initConsumerProperties(final Configuration config) {
        //key.deserializer
        this.consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DEFAULT_DESERIALIZER);
        //value.deserializer
        this.consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DEFAULT_DESERIALIZER);
        //auto.offset.reset
        this.consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, DEFAULT_AUTO_OFFSET_RESET_CONFIG);
        //enable.auto.commit
        this.consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, DEFAULT_ENABLE_AUTO_COMMIT_CONFIG);
        //cover all kafka consumer properties and auth config may change
        config.asMap().forEach((k, v) -> {
            if (k.startsWith(TidbConnectorConfig.KAFKA_CONSUMER_PREFIX)) {
                this.consumerProperties.put(k.substring(TidbConnectorConfig.KAFKA_CONSUMER_PREFIX.length()), v);
            }
        });
    }

    @Override
    public void doReading() {
        status = ReaderStatus.RUNNING;
        Runnable runner = getRunner();
        for (int i = 0; i < totalRunnerCount.get(); i++) {
            String threadName = "reader-runner-" + consumerProperties.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
                    + "-" + consumerProperties.get(ConsumerConfig.GROUP_ID_CONFIG) + "-" + i;
            threadsHolder.put(newNamedThread(runner, threadName), NULL_CONSUMER_PLACEHOLDER);
        }
        threadsHolder.keySet().forEach(Thread::start);
    }

    private Runnable getRunner() {
        return () -> {
            runnerLifecycleListeners.forEach(RunnerLifecycleListener::onStart);
            KafkaConsumer<byte[], byte[]> consumer = null;
            Throwable cause = null;
            try {
                consumer = new KafkaConsumer<>(this.consumerProperties);
                threadsHolder.put(Thread.currentThread(), consumer);
                KafkaConsumer<byte[], byte[]> finalConsumer = consumer;
                consumer.subscribe(Collections.singletonList(subscribeTopic), new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
                        List<Integer> revokedPartitions = partitions.stream().map(TopicPartition::partition).collect(Collectors.toList());
                        offsetManager.clear(revokedPartitions);
                    }

                    @Override
                    public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
                        ConcurrentMap<Integer, Long> partitionOffsets = getCommittedPartitionOffsets(partitions, finalConsumer);
                        offsetManager.init(partitionOffsets);
                    }
                });
                int retryTimes = 0;
                while (true) {
                    try {
                        pollAndHandleWithKafkaRecords(consumer);
                        retryTimes = 0;
                    } catch (WakeupException e) {
                        log.warn("Reader runner is stopping ...");
                        throw e;
                    } catch (KafkaException e) {
                        log.error("Reader runner throws kafka exception:{}, message:{}", Arrays.toString(e.getStackTrace()), e.getMessage());
                        retryIntervalDelayStrategy.sleepWhen(true);
                        if (retryTimes++ >= MAX_RETRY_TIMES) {
                            String msg = String.format("Reader runner has retried %d times", retryTimes - 1);
                            log.warn(msg);
                            throw new KafkaException(msg, e);
                        }
                    }
                }
            } catch (WakeupException exception) {
                log.warn("Reader runner is stopping for consumer being wake up.");
                cause = exception;
            } catch (KafkaException kafkaException) {
                log.error("Reader runner is stopping for {}", kafkaException.getMessage());
                cause = kafkaException;
            } finally {
                runnerLifecycleListeners.forEach(RunnerLifecycleListener::onEnd);
                Event endRunnerEvent = new Event(EventType.RUNNER_STOP_EVENT, cause);
                eventListeners.forEach(eventListener -> eventListener.onEvent(endRunnerEvent));
                log.warn("Runner {} is down.", Thread.currentThread().getName());
                if (consumer != null) {
                    consumer.close();
                }
            }
        };
    }

    private void pollAndHandleWithKafkaRecords(final KafkaConsumer<byte[], byte[]> consumer) {
        if (ReaderStatus.STOPPED == status) {
            runnerLifecycleListeners.forEach(RunnerLifecycleListener::triggerStop);
        }
        if (offsetManager.ifNeedCommit()) {
            doCommit(consumer, offsetManager);
        }
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(DEFAULT_CONSUMER_POLL_DURATION));
        for (ConsumerRecord<byte[], byte[]> record : records) {
            KafkaMessage kafkaMessage = recordToKafkaMessage(record);
            List<Event> events = TicdcOpenProtocolParser.parse(kafkaMessage);
            Set<Integer> eventIds = events.stream().map(Event::getOrder).collect(Collectors.toSet());
            offsetManager.add(record.partition(), record.offset(), eventIds);
            events.forEach(event -> eventListeners.forEach(eventListener -> eventListener.onEvent(event)));
        }
    }

    private ConcurrentMap<Integer, Long> getCommittedPartitionOffsets(final Collection<TopicPartition> partitions, final KafkaConsumer<byte[], byte[]> finalConsumer) {
        Map<TopicPartition, OffsetAndMetadata> committed = finalConsumer.committed(new HashSet<>(partitions));
        return committed.entrySet().stream().collect(Collectors.toConcurrentMap(entry -> entry.getKey().partition(),
            entry -> {
                Long partitionBeginningOffset = getBeginningOffset(finalConsumer, entry.getKey());
                if (entry.getValue() == null) {
                    return partitionBeginningOffset;
                }
                return partitionBeginningOffset > entry.getValue().offset() ? partitionBeginningOffset : entry.getValue().offset();
            }
        ));
    }

    private void doCommit(final KafkaConsumer<byte[], byte[]> consumer, final KafkaReaderOffsetManager offsetManager) {
        Map<Integer, Long> toCommitOffsets = offsetManager.getToCommitOffsets();
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = toCommitOffsets.entrySet().stream()
                .collect(Collectors.toMap(
                    entry -> new TopicPartition(subscribeTopic, entry.getKey()),
                    entry -> new OffsetAndMetadata(entry.getValue())
                ));
        consumer.commitSync(topicPartitionOffsets);
    }

    private Long getBeginningOffset(final KafkaConsumer<byte[], byte[]> consumer, final TopicPartition topicPartition) {
        Map<TopicPartition, Long> partitionOffset = consumer.beginningOffsets(Lists.newArrayList(topicPartition));
        return partitionOffset.get(topicPartition);
    }

    private KafkaMessage recordToKafkaMessage(final ConsumerRecord<byte[], byte[]> record) {
        return new KafkaMessage() {{
                setKey(record.key());
                setValue(record.value());
                setOffset(record.offset());
                setPartition(record.partition());
                setTimestamp(record.timestamp());
            }};
    }

    private Thread newNamedThread(final Runnable runnable, final String threadName) {
        Thread thread = new Thread(runnable);
        thread.setName(threadName);
        return thread;
    }

    @Override
    public void registerEventListener(final EventListener listener) {
        eventListeners.add(listener);
    }

    @Override
    public void unregisterEventListener() {
        eventListeners.clear();
    }

    @Override
    public void registerRunnerLifecycleListener(final RunnerLifecycleListener listener) {
        runnerLifecycleListeners.add(listener);
    }

    @Override
    public void unregisterRunnerLifecycleListener() {
        runnerLifecycleListeners.clear();
    }

    @Override
    public int getTotalRunnerCount() {
        return totalRunnerCount.get();
    }

    @Override
    public void setTotalRunnerCount(final int totalRunnerCount) {
        this.totalRunnerCount.set(totalRunnerCount);
    }

    @Override
    public int getAliveRunnerCount() {
        return aliveRunnerCount.get();
    }

    @Override
    public Map<Integer, Long> getToCommitOffset() {
        return offsetManager.getToCommitOffsets();
    }

    @Override
    public void close() throws InterruptedException {
        unregisterEventListener();
        status = ReaderStatus.STOPPED;
        for (Map.Entry<Thread, Object> entry : threadsHolder.entrySet()) {
            Thread thread = entry.getKey();
            Object consumer = entry.getValue();
            if (thread.isAlive() && consumer instanceof KafkaConsumer) {
                ((KafkaConsumer) consumer).wakeup();
            }
            thread.join(RUNNER_STOP_TIMEOUT);
        }
        unregisterRunnerLifecycleListener();
    }

    @Override
    public void markTicdcEventAsDone(int partition, long offset, int orderInBatch) {
        offsetManager.markTicdcEventAsDone(partition, offset, orderInBatch);
    }

}
