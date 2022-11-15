package cn.xdf.acdc.connector.tidb.source;

import cn.xdf.acdc.connector.tidb.KafkaEnv;
import cn.xdf.acdc.connector.tidb.util.DelayStrategy;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TidbConnectorTaskTest extends KafkaEnv {

    private final DelayStrategy delayStrategy = DelayStrategy.exponentialWithTimeoutException(30_000);

    private TidbConnectorTask getTidbConnectorTask() {
        return new TidbConnectorTask();
    }

    @Test
    public void testDoPollShouldPollRecordInOrder() throws InterruptedException {
        TidbConnectorTask tidbConnectorTask = getTidbConnectorTask();
        tidbConnectorTask.start(TidbConnectorConfigTest.getTidbConnectorConfig(getKafka().brokerList(), SOURCE_TOPIC_NAME, "consumerGroupId", 1).getConfig());
        Assert.assertNotNull(tidbConnectorTask.version());
        Assert.assertNotNull(tidbConnectorTask.getAllConfigurationFields());
        List<SourceRecord> records = new ArrayList<>();
        records.addAll(tidbConnectorTask.doPoll());
        Thread.sleep(2000L);
        delayStrategy.sleepWhenNotTimeout(() -> {
            try {
                records.addAll(tidbConnectorTask.doPoll());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return records.size() != 5;
        });
        Assert.assertEquals(5, records.size());
        Assert.assertEquals(10, ((Struct) ((Struct) records.get(0).value()).get("after")).get("col1"));
        Assert.assertEquals(11, ((Struct) ((Struct) records.get(1).value()).get("after")).get("col1"));
        Assert.assertEquals(12, ((Struct) ((Struct) records.get(2).value()).get("after")).get("col1"));
        Assert.assertEquals(13, ((Struct) ((Struct) records.get(3).value()).get("after")).get("col1"));
        Assert.assertEquals(14, ((Struct) ((Struct) records.get(4).value()).get("after")).get("col1"));

        tidbConnectorTask.commitRecord(records.get(0));
        tidbConnectorTask.commitRecord(records.get(1));
        tidbConnectorTask.commitRecord(records.get(2));
        tidbConnectorTask.commitRecord(records.get(3));
        tidbConnectorTask.commitRecord(records.get(4));

        tidbConnectorTask.doStop();
    }

}
