package cn.xdf.acdc.connector.tidb.reader;

import cn.xdf.acdc.connector.tidb.KafkaEnv;
import cn.xdf.acdc.connector.tidb.source.TidbConnectorConfig;
import cn.xdf.acdc.connector.tidb.source.TidbConnectorConfigTest;
import lombok.Getter;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicInteger;

@Getter
public class BaseReaderWithKafkaEnv extends KafkaEnv {

    protected static final AtomicInteger READER_ID = new AtomicInteger(0);

    private KafkaTidbOpenProtocolReader reader;

    @Before
    public void newReader() {
        TidbConnectorConfig tidbConnectorConfig = TidbConnectorConfigTest.getTidbConnectorConfig(getKafka().brokerList(), SOURCE_TOPIC_NAME, "reader-" + READER_ID.incrementAndGet(), 1);
        reader = new KafkaTidbOpenProtocolReader(tidbConnectorConfig.getConfig());
    }

    @After
    public void closeReader() throws InterruptedException {
        reader.close();
    }

}
