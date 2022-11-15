package cn.xdf.acdc.connect.kafka.sink;

import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import cn.xdf.acdc.connect.kafka.util.Version;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class KafkaSinkTask extends SinkTask {

    private KafkaSinkConfig kafkaSinkConfig;

    private KafkaWriter kafkaWriter;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(final Map<String, String> props) {
        log.info("Starting kafka sink task...");
        kafkaSinkConfig = new KafkaSinkConfig(props);
        Map<String, Object> producerConfigs = getProducerConfigs(kafkaSinkConfig);

        kafkaWriter = new KafkaWriter(kafkaSinkConfig, getSinkKeyConvertor(), getSinkValueConvertor(), new KafkaProducer<>(producerConfigs),
                KafkaWriter.RECORD_FORWARD_RESULT_QUEUE_OVERLOAD_TOLERANCE_TIME_IN_SECONDS, KafkaWriter.RECORD_FORWARD_RESULT_QUEUE_CAPACITY);
    }

    @SneakyThrows
    @Override
    public void put(final Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        kafkaWriter.write(records);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        return kafkaWriter.getToBeCommittedOffsets();
    }

    @Override
    public void close(final Collection<TopicPartition> partitions) {
        kafkaWriter.closePartitions(partitions);
    }

    @Override
    public void stop() {
        kafkaWriter.close();
        log.info("Stopping kafka sink task.");
    }

    /**
     * Get kafka writer.
     *
     * @return kafka writer
     */
    protected KafkaWriter getKafkaWriter() {
        return this.kafkaWriter;
    }

    private Converter getSinkValueConvertor() {
        return getConverter(false, KafkaSinkConfig.SINK_KAFKA_VALUE_CONVERTER);
    }

    private Converter getSinkKeyConvertor() {
        return getConverter(true, KafkaSinkConfig.SINK_KAFKA_KEY_CONVERTER);
    }

    private Converter getConverter(final boolean isKey, final String configName) {
        String converterClassName = kafkaSinkConfig.getString(configName);

        Converter converter = (Converter) newInstance(converterClassName);
        converter.configure(kafkaSinkConfig.originalsWithPrefix(configName), isKey);

        return converter;
    }

    private Object newInstance(final String className) {
        try {
            Class<?> clz = Class.forName(className);
            return clz.newInstance();
        } catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
            throw new ConnectException(e);
        }
    }

    protected Map<String, Object> getProducerConfigs(final SinkConfig config) {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        // These settings will execute infinite retries on retriable exceptions. They *may* be overridden via configs.
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(Long.MAX_VALUE));
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        // todo 最低兼容
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));

        producerProps.putAll(config.originalsWithPrefix(KafkaSinkConfig.SINK_KAFKA_PREFIX));

        return producerProps;
    }
}
