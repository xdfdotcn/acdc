package cn.xdf.acdc.connector.tidb;

import cn.xdf.acdc.connector.tidb.ticdc.parser.TicdcOpenProtocolParserTest;
import com.pingcap.ticdc.cdc.KafkaMessage;
import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Collect;
import io.debezium.util.Testing;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaEnv {

    protected static final String SOURCE_TOPIC_NAME = "ticdc_source_data";

    private static KafkaCluster kafka;

    @BeforeClass
    public static void startKafka() throws Exception {
        File dataDir = Testing.Files.createTestingDirectory("ticdc_source_data");
        Testing.Files.delete(dataDir);

        // Configure the extra properties to
        kafka = new KafkaCluster().usingDirectory(dataDir)
                .deleteDataPriorToStartup(true)
                .deleteDataUponShutdown(true)
                .addBrokers(1)
                .withKafkaConfiguration(Collect.propertiesOf(
                        "auto.create.topics.enable", "false",
                        "zookeeper.session.timeout.ms", "20000"))
                .startup();
        produceTopicRecords();
    }

    private static void produceTopicRecords() throws IOException, InterruptedException {
        kafka.createTopic(SOURCE_TOPIC_NAME, 1, 1);
        List<KafkaMessage> kafkaMessagesFromTestData = TicdcOpenProtocolParserTest.getKafkaMessagesFromTestData(null);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.brokerList());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);
        CountDownLatch cdl = new CountDownLatch(kafkaMessagesFromTestData.size());
        kafkaMessagesFromTestData.forEach(kafkaMessage -> producer.send(
            new ProducerRecord<>(SOURCE_TOPIC_NAME, kafkaMessage.getKey(), kafkaMessage.getValue()),
            (recordMetadata, e) -> cdl.countDown()));
        cdl.await();
    }

    @AfterClass
    public static void stopKafka() {
        if (kafka != null) {
            kafka.shutdown();
        }
    }

    protected static KafkaCluster getKafka() {
        return kafka;
    }

}
