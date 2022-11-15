package cn.xdf.acdc.connect.kafka.sink;

import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import cn.xdf.acdc.connect.kafka.sink.utils.KafkaSinkTestUtil;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class KafkaSinkTaskTest {

    private KafkaSinkTask kafkaSinkTask;

    @Before
    public void setup() {
        kafkaSinkTask = new KafkaSinkTask();
    }

    @Test
    public void testConvertersShouldBeInstantiatedAsExpected() {
        Map<String, String> prop = KafkaSinkTestUtil.fakeKafkaSinkTaskConfig();
        kafkaSinkTask.start(prop);
        KafkaWriter kafkaWriter = kafkaSinkTask.getKafkaWriter();
        Assert.assertTrue(kafkaWriter.getKeyConverter() instanceof JsonConverter);
        Assert.assertTrue(kafkaWriter.getValueConverter() instanceof JsonConverter);
    }

    @Test(expected = ConnectException.class)
    public void testConvertersShouldThrowExceptionWithConfigErrors() {
        Map<String, String> prop = KafkaSinkTestUtil.fakeKafkaSinkTaskConfig();
        prop.put("sink.kafka.key.converter", "xxxx");
        kafkaSinkTask.start(prop);
    }

    @Test
    public void testKafkaProducerConfigShouldSetAsExpected() {
        Map<String, String> prop = KafkaSinkTestUtil.fakeKafkaSinkTaskConfig();
        SinkConfig kafkaSinkConfig = new KafkaSinkConfig(prop);
        Map<String, Object> producerConfigs = kafkaSinkTask.getProducerConfigs(kafkaSinkConfig);
        Assert.assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer", producerConfigs.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        Assert.assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer", producerConfigs.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        Assert.assertEquals(String.valueOf(Integer.MAX_VALUE), producerConfigs.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG));
        Assert.assertEquals(String.valueOf(Long.MAX_VALUE), producerConfigs.get(ProducerConfig.MAX_BLOCK_MS_CONFIG));
        Assert.assertEquals("all", producerConfigs.get(ProducerConfig.ACKS_CONFIG));
        Assert.assertEquals("1", producerConfigs.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION));
        Assert.assertEquals(String.valueOf(Integer.MAX_VALUE), producerConfigs.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG));
        Assert.assertEquals("localhost:9092", producerConfigs.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        Assert.assertEquals("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"xxx\" password=\"xxx\";", producerConfigs.get("sasl.jaas.config"));
        Assert.assertEquals("SASL_PLAINTEXT", producerConfigs.get("security.protocol"));
        Assert.assertEquals("SCRAM-SHA-512", producerConfigs.get("sasl.mechanism"));
    }

    @Test
    public void testKafkaProducerConfigShouldBeOverrideByCustomConfig() {
        Map<String, String> prop = KafkaSinkTestUtil.fakeKafkaSinkTaskConfig();
        prop.put(KafkaSinkConfig.SINK_KAFKA_PREFIX + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        SinkConfig kafkaSinkConfig = new KafkaSinkConfig(prop);
        Map<String, Object> producerConfigs = kafkaSinkTask.getProducerConfigs(kafkaSinkConfig);

        Assert.assertEquals("5", producerConfigs.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION));
    }
}
