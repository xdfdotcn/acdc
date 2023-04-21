package cn.xdf.acdc.connector.tidb.source;

import io.debezium.config.Configuration;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.relational.TableId;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.Properties;

public class TidbOffsetContextTest {

    @Test
    public void testSourceStructShouldBeWithAName() {
        TidbConnectorConfig config = getTidbConnectorConfig("test-bootstrap", "test-topic", "test-group", 1);
        TidbOffsetContext tidbOffsetContext = new TidbOffsetContext(config);
        Assert.assertEquals("cn.xdf.acdc.connector.tidb.Source", tidbOffsetContext.getSourceInfoSchema().name());
        Assert.assertEquals(new TidbSourceInfoStructMaker().struct(null), tidbOffsetContext.getSourceInfo());
    }

    @Test
    public void testSourceStructShouldBeFilledWithATableName() {
        TidbConnectorConfig config = getTidbConnectorConfig("test-bootstrap", "test-topic", "test-group", 1);
        TidbOffsetContext tidbOffsetContext = new TidbOffsetContext(config);
        TableId tableId = new TableId("test-catalog", "test-schema", "test-table");
        tidbOffsetContext.event(tableId, Instant.now());
        Assert.assertEquals("test-table", tidbOffsetContext.getSourceInfo().get(AbstractSourceInfo.TABLE_NAME_KEY));
    }

    /**
     * Get common tidb connector config.
     *
     * @param sourceBootstrap source kafka bootstrap
     * @param sourceTopic source kafka topic
     * @param consumerGroupId source consumer group id
     * @param threadNumber thread number
     * @return common tidb connector config
     */
    public static TidbConnectorConfig getTidbConnectorConfig(final String sourceBootstrap, final String sourceTopic, final String consumerGroupId, final int threadNumber) {
        Properties properties = new Properties();
        properties.setProperty("connector.class", "cn.xdf.acdc.connector.tidb.TidbConnector");
        properties.setProperty("tasks.max", "1");
        properties.setProperty("database.server.name", "cdc_tidb_unit_test");
        properties.setProperty("source.kafka.bootstrap.servers", sourceBootstrap);
        properties.setProperty("source.kafka.topic", sourceTopic);
        properties.setProperty("source.kafka.group.id", consumerGroupId);
        properties.setProperty("database.include", "a,database_name");
        properties.setProperty("table.include.list", "a.b,a.c,database_name.table_name");
        properties.setProperty("message.key.columns", "a.b:col1,a.c:col1");
        properties.setProperty("time.precision.mode", "connect");
        properties.setProperty("transforms", "route,unwrap");
        properties.setProperty("transforms.route.type", "org.apache.kafka.connect.transforms.RegexRouter");
        properties.setProperty("transforms.route.regex", "([^.]+)\\.([^.]+)\\.([^.]+)");
        properties.setProperty("transforms.route.replacement", "$1-$2-$3");
        properties.setProperty("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
        properties.setProperty("transforms.unwrap.delete.handling.mode", "rewrite");
        properties.setProperty("transforms.unwrap.add.fields", "op");
        properties.setProperty("topic.creation.default.replication.factor", "3");
        properties.setProperty("topic.creation.default.partitions", "1");
        properties.setProperty("topic.creation.default.cleanup.policy", "compact");
        properties.setProperty("topic.creation.default.compression.type", "lz4");
        properties.setProperty("topic.creation.groups", "data");
        properties.setProperty("topic.creation.data.include", "cdc_tidb_unit_test-.*");
        properties.setProperty("topic.creation.data.replication.factor", "3");
        properties.setProperty("topic.creation.data.partitions", "3");
        properties.setProperty("topic.creation.data.cleanup.policy", "delete");
        properties.setProperty("topic.creation.data.compression.type", "lz4");
        properties.setProperty("topic.creation.data.retention.ms", "86400000");
        properties.setProperty("producer.compression.type", "lz4");
        properties.setProperty("key.converter", "io.confluent.connect.avro.AvroConverter");
        properties.setProperty("value.converter", "io.confluent.connect.avro.AvroConverter");
        properties.setProperty("key.converter.schema.registry.url", "http://schema-registry:8081");
        properties.setProperty("value.converter.schema.registry.url", "http://schema-registry:8081");
        properties.setProperty("source.kafka.reader.thread.number", Integer.toString(threadNumber));
        Configuration configuration = Configuration.from(properties);
        return new TidbConnectorConfig(configuration);
    }
}
