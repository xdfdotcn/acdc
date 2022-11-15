package cn.xdf.acdc.connector.tidb.ticdc.parser;

import cn.xdf.acdc.connector.tidb.reader.Event;
import cn.xdf.acdc.connector.tidb.reader.EventType;
import com.pingcap.ticdc.cdc.KafkaMessage;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TicdcOpenProtocolParserTest {
    @Test
    public void testParseShouldMatchExpectResult() throws IOException {
        List<KafkaMessage> kafkaMessagesFromTestData = getKafkaMessagesFromTestData("ddl_0");
        for (KafkaMessage kafkaMessage : kafkaMessagesFromTestData) {
            List<Event> parse = TicdcOpenProtocolParser.parse(kafkaMessage);
            Assert.assertEquals(1, parse.size());
            Assert.assertEquals(EventType.DDL_EVENT, parse.get(0).getType());
        }

        kafkaMessagesFromTestData = getKafkaMessagesFromTestData("ddl_1");
        for (KafkaMessage kafkaMessage : kafkaMessagesFromTestData) {
            List<Event> parse = TicdcOpenProtocolParser.parse(kafkaMessage);
            Assert.assertEquals(3, parse.size());
            Assert.assertEquals(EventType.DDL_EVENT, parse.get(0).getType());
            Assert.assertEquals(EventType.DDL_EVENT, parse.get(1).getType());
            Assert.assertEquals(EventType.DDL_EVENT, parse.get(2).getType());
        }

        kafkaMessagesFromTestData = getKafkaMessagesFromTestData("row_0");
        for (KafkaMessage kafkaMessage : kafkaMessagesFromTestData) {
            List<Event> parse = TicdcOpenProtocolParser.parse(kafkaMessage);
            Assert.assertEquals(1, parse.size());
            Assert.assertEquals(EventType.ROW_CHANGED_EVENT, parse.get(0).getType());
        }

        kafkaMessagesFromTestData = getKafkaMessagesFromTestData("row_1");
        for (KafkaMessage kafkaMessage : kafkaMessagesFromTestData) {
            List<Event> parse = TicdcOpenProtocolParser.parse(kafkaMessage);
            Assert.assertEquals(4, parse.size());
            Assert.assertEquals(EventType.ROW_CHANGED_EVENT, parse.get(0).getType());
            Assert.assertEquals(EventType.ROW_CHANGED_EVENT, parse.get(1).getType());
            Assert.assertEquals(EventType.ROW_CHANGED_EVENT, parse.get(2).getType());
            Assert.assertEquals(EventType.ROW_CHANGED_EVENT, parse.get(3).getType());
        }

        kafkaMessagesFromTestData = getKafkaMessagesFromTestData("rts_0");
        for (KafkaMessage kafkaMessage : kafkaMessagesFromTestData) {
            List<Event> parse = TicdcOpenProtocolParser.parse(kafkaMessage);
            Assert.assertEquals(1, parse.size());
            Assert.assertEquals(EventType.RESOLVED_EVENT, parse.get(0).getType());
        }

        kafkaMessagesFromTestData = getKafkaMessagesFromTestData("rts_1");
        for (KafkaMessage kafkaMessage : kafkaMessagesFromTestData) {
            List<Event> parse = TicdcOpenProtocolParser.parse(kafkaMessage);
            Assert.assertEquals(3, parse.size());
            Assert.assertEquals(EventType.RESOLVED_EVENT, parse.get(0).getType());
            Assert.assertEquals(EventType.RESOLVED_EVENT, parse.get(1).getType());
            Assert.assertEquals(EventType.RESOLVED_EVENT, parse.get(2).getType());
        }
    }

    /**
     * Mock Kafka messages, which usually consumed from kafka.
     *
     * @param fileName file name ,keep null mean all messages in the path
     * @return kafka message list
     * @throws IOException io exception
     */
    public static List<KafkaMessage> getKafkaMessagesFromTestData(final String fileName) throws IOException {
        List<KafkaMessage> kafkaMessages = new ArrayList<>();

        File keyFolder = getClasspathFile("data/key");
        File[] keyFiles = keyFolder.listFiles(pathname -> {
            if (fileName == null || pathname.getName().equals(fileName)) {
                return true;
            }
            return false;
        });
        Arrays.sort(keyFiles);
        File valueFolder = getClasspathFile("data/value");
        File[] valueFiles = valueFolder.listFiles(pathname -> {
            if (fileName == null || pathname.getName().equals(fileName)) {
                return true;
            }
            return false;
        });
        Arrays.sort(valueFiles);
        Assert.assertNotNull(keyFiles);
        Assert.assertNotNull(valueFiles);
        Assert.assertEquals(keyFiles.length, valueFiles.length);

        for (int i = 0; i < keyFiles.length; i++) {
            File kf = keyFiles[i];
            byte[] kafkaMessageKey = Files.readAllBytes(kf.toPath());
            File vf = valueFiles[i];
            byte[] kafkaMessageValue = Files.readAllBytes(vf.toPath());
            KafkaMessage kafkaMessage = new KafkaMessage(kafkaMessageKey, kafkaMessageValue);
            kafkaMessage.setPartition(1);
            kafkaMessage.setOffset(1L);
            kafkaMessage.setTimestamp(System.currentTimeMillis());
            kafkaMessages.add(kafkaMessage);
        }
        return kafkaMessages;
    }

    private static File getClasspathFile(final String path) {
        ClassLoader classLoader = TicdcOpenProtocolParserTest.class.getClassLoader();
        URL url = classLoader.getResource(path);
        Assert.assertNotNull(url);
        return new File(url.getFile());
    }
}
