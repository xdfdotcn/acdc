package cn.xdf.acdc.connect.kafka.sink;

import cn.xdf.acdc.connect.core.sink.AbstractWriter;
import cn.xdf.acdc.connect.plugins.converter.xdf.RecordConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
public class KafkaWriter extends AbstractWriter<KafkaProducer<byte[], byte[]>> {

    public static final int RECORD_FORWARD_RESULT_QUEUE_OVERLOAD_TOLERANCE_TIME_IN_SECONDS = 30;

    public static final int RECORD_FORWARD_RESULT_QUEUE_CAPACITY = 10000;

    public static final int PRODUCER_CLOSE_TIMEOUT_IN_SECONDS = 30;

    private final Converter keyConverter;

    private final Converter valueConverter;

    private final Map<TopicPartition, Queue<RecordForwardResult>> recordForwardResultQueues = new HashMap<>();

    private final AtomicReference<Exception> producerSendException;

    private final Map<TopicPartition, Long> currentOffsets = new HashMap<>();

    private final KafkaProducer<byte[], byte[]> kafkaProducer;

    private final int recordForwardResultQueueOverloadTolerateMaxInSeconds;

    private final int recordForwardResultQueueCapacity;

    public KafkaWriter(final KafkaSinkConfig kafkaSinkConfig, final Converter keyConvertor, final Converter valueConverter,
                       final KafkaProducer<byte[], byte[]> kafkaProducer, final int recordForwardResultQueueOverloadTolerateMaxInSeconds,
                       final int recordForwardResultQueueCapacity) {
        super(kafkaSinkConfig);
        this.producerSendException = new AtomicReference<>();
        this.keyConverter = keyConvertor;
        this.valueConverter = valueConverter;
        this.kafkaProducer = kafkaProducer;
        this.recordForwardResultQueueOverloadTolerateMaxInSeconds = recordForwardResultQueueOverloadTolerateMaxInSeconds;
        this.recordForwardResultQueueCapacity = recordForwardResultQueueCapacity;
    }

    /**
     * Get key converter.
     *
     * @return key converter
     */
    protected Converter getKeyConverter() {
        return this.keyConverter;
    }

    /**
     * Get value converter.
     *
     * @return value converter
     */
    protected Converter getValueConverter() {
        return this.valueConverter;
    }

    @Override
    protected KafkaProducer<byte[], byte[]> getClient() {
        return kafkaProducer;
    }

    @Override
    protected void doWrite(final KafkaProducer<byte[], byte[]> client, final String target, final SinkRecord record) {
        if (producerSendException.get() != null) {
            throw new ConnectException("Unrecoverable exception from producer send callback", producerSendException.get());
        }

        final ProducerRecord<byte[], byte[]> producerRecord = convertToProducerRecord(record, target);

        // for original offset commit
        RecordForwardResult recordForwardResult = buildRecordForwardResultAndOfferToQueue(record);

        client.send(producerRecord, (recordMetadata, e) -> {
            if (e != null) {
                log.error("{} failed to send record to {}: ", KafkaWriter.this, "topic", e);
                producerSendException.compareAndSet(null, e);
            }
            recordForwardResult.done(e);
        });
    }

    private ProducerRecord<byte[], byte[]> convertToProducerRecord(final SinkRecord record, final String topic) {
        if (keyConverter instanceof RecordConverter) {
            List<String> ids = record.keySchema().fields().stream().map(Field::name).collect(Collectors.toList());
            return ((RecordConverter) keyConverter).fromRecordData(topic, ids, record.valueSchema(), (Struct) record.value(), record.kafkaOffset());
        }

        byte[] key = keyConverter.fromConnectData(topic, record.keySchema(), record.key());
        byte[] value = valueConverter.fromConnectData(topic, record.valueSchema(), record.value());
        return new ProducerRecord<>(topic, key, value);
    }

    private RecordForwardResult buildRecordForwardResultAndOfferToQueue(final SinkRecord record) {
        RecordForwardResult recordForwardResult = new RecordForwardResult(record.kafkaOffset());

        TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
        Queue<RecordForwardResult> recordForwardResultQueue = recordForwardResultQueues.computeIfAbsent(tp, key -> new LinkedBlockingQueue<>(recordForwardResultQueueCapacity));

        while (!recordForwardResultQueue.offer(recordForwardResult)) {
            refreshCurrentOffset(tp, recordForwardResultQueue, true);
        }
        return recordForwardResult;
    }

    private boolean refreshCurrentOffset(final TopicPartition tp, final Queue<RecordForwardResult> recordForwardResultQueue, final boolean wait) {
        RecordForwardResult recordForwardResult = pollHeadIfIsDone(recordForwardResultQueue, wait);
        if (Objects.nonNull(recordForwardResult)) {
            currentOffsets.put(tp, recordForwardResult.getUpstreamOffset());
            return true;
        }
        return false;
    }

    private RecordForwardResult pollHeadIfIsDone(final Queue<RecordForwardResult> recordForwardResultQueue, final boolean wait) {
        RecordForwardResult recordForwardResult = recordForwardResultQueue.peek();
        if (Objects.nonNull(recordForwardResult)) {
            if (recordForwardResult.isDone() || wait) {
                try {
                    recordForwardResult.get(recordForwardResultQueueOverloadTolerateMaxInSeconds, TimeUnit.SECONDS);
                    return recordForwardResultQueue.poll();
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    throw new ConnectException(e);
                }
            }
        }
        return null;
    }

    /**
     * Get current offsets.
     *
     * @return current offsets
     */
    protected Map<TopicPartition, Long> getCurrentOffsets() {
        return this.currentOffsets;
    }

    /**
     * Get record forward result queues.
     *
     * @return record forward result queues
     */
    protected Map<TopicPartition, Queue<RecordForwardResult>> getRecordForwardResultQueues() {
        return this.recordForwardResultQueues;
    }

    /**
     * Refresh and get current offsets.
     *
     * @return get current offsets
     */
    public Map<TopicPartition, OffsetAndMetadata> getToBeCommittedOffsets() {
        refreshCurrentOffsets();
        return currentOffsets.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> new OffsetAndMetadata(entry.getValue() + 1)));
    }

    private void refreshCurrentOffsets() {
        recordForwardResultQueues.forEach((tp, recordForwardResultQueue) -> {
            while (refreshCurrentOffset(tp, recordForwardResultQueue, false)) {
                log.debug("refresh current Offset, topic partition: {}", tp);
            }
        });
    }

    @Override
    public void closePartitions(final Collection<TopicPartition> partitions) {
        partitions.forEach(tp -> {
            recordForwardResultQueues.remove(tp);
            currentOffsets.remove(tp);
        });
    }

    @Override
    public void close() {
        kafkaProducer.close(Duration.ofSeconds(PRODUCER_CLOSE_TIMEOUT_IN_SECONDS));
    }
}
