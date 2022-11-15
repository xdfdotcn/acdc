package cn.xdf.acdc.connector.tidb.reader;

import cn.xdf.acdc.connector.tidb.source.TidbConnectorConfig;
import cn.xdf.acdc.connector.tidb.source.TidbConnectorConfigTest;
import cn.xdf.acdc.connector.tidb.util.ReflectionUtils;
import com.google.common.collect.Lists;
import com.pingcap.ticdc.cdc.TicdcEventData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaTidbOpenProtocolReaderTest extends BaseReaderWithKafkaEnv {

    @Test
    public void testDoReadingShouldThrowKafkaExceptionWithInvalidKafkaConfig() throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        TidbConnectorConfig tidbConnectorConfig = TidbConnectorConfigTest.getTidbConnectorConfig("invalid-kafka-broker", SOURCE_TOPIC_NAME, "reader-" + READER_ID.incrementAndGet(), 1);
        TidbDataReader reader = new KafkaTidbOpenProtocolReader(tidbConnectorConfig.getConfig());
        AtomicReference<Event> resultEvent = new AtomicReference<>();
        reader.registerEventListener(event -> resultEvent.set(event));
        reader.doReading();
        Map<Thread, Object> threadsHolder = (Map<Thread, Object>) ReflectionUtils.getLimitedAccessField(reader, "threadsHolder");
        Thread thread = (Thread) threadsHolder.keySet().toArray()[0];
        long startTime = System.currentTimeMillis();
        thread.join(10000);
        Assert.assertTrue(System.currentTimeMillis() - startTime < 10000);
        Assert.assertEquals(EventType.RUNNER_STOP_EVENT, resultEvent.get().getType());
        Assert.assertTrue(resultEvent.get().getData() instanceof KafkaException);
    }

    @Test
    public void testDoReadingShouldThrowWakeUpExceptionWithThreadInterrupt() throws NoSuchFieldException, IllegalAccessException, InterruptedException {
        AtomicReference<Event> resultEvent = new AtomicReference<>();
        getReader().registerEventListener(resultEvent::set);
        getReader().doReading();
        Map<Thread, Object> threadsHolder = (Map<Thread, Object>) ReflectionUtils.getLimitedAccessField(getReader(), "threadsHolder");
        Thread thread = (Thread) threadsHolder.keySet().toArray()[0];
        Thread.sleep(500L);
        Assert.assertTrue(threadsHolder.values().toArray()[0] instanceof KafkaConsumer);
        ((KafkaConsumer) threadsHolder.values().toArray()[0]).wakeup();
        long startTime = System.currentTimeMillis();
        thread.join(10000);
        Assert.assertTrue(System.currentTimeMillis() - startTime < 10000);
        Assert.assertEquals(EventType.RUNNER_STOP_EVENT, resultEvent.get().getType());
        Assert.assertTrue(resultEvent.get().getData() instanceof WakeupException);
    }

    @Test
    public void testDoReadingShouldRetryWithKafkaException() throws NoSuchFieldException, IllegalAccessException, InterruptedException {
        AtomicReference<Event> resultEvent = new AtomicReference<>();
        getReader().registerEventListener(resultEvent::set);
        getReader().doReading();
        Map<Thread, Object> threadsHolder = (Map<Thread, Object>) ReflectionUtils.getLimitedAccessField(getReader(), "threadsHolder");
        Thread thread = (Thread) threadsHolder.keySet().toArray()[0];
        Thread.sleep(500L);
        Assert.assertTrue(threadsHolder.values().toArray()[0] instanceof KafkaConsumer);
        thread.interrupt();
        long startTime = System.currentTimeMillis();
        thread.join(10000);
        Assert.assertTrue(System.currentTimeMillis() - startTime < 10000);
        Assert.assertEquals(EventType.RUNNER_STOP_EVENT, resultEvent.get().getType());
        Assert.assertTrue(resultEvent.get().getData() instanceof KafkaException);
        Assert.assertEquals("Reader runner has retried 3 times", ((KafkaException) resultEvent.get().getData()).getMessage());
    }

    @Test
    public void testCloseShouldCloseRunnerAndClearListeners() throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        getReader().doReading();
        getReader().close();
        Map<Thread, Object> threadsHolder = (Map<Thread, Object>) ReflectionUtils.getLimitedAccessField(getReader(), "threadsHolder");
        List<EventListener> eventListeners = (List<EventListener>) ReflectionUtils.getLimitedAccessField(getReader(), "eventListeners");
        List<RunnerLifecycleListener> runnerLifecycleListeners = (List<RunnerLifecycleListener>) ReflectionUtils.getLimitedAccessField(getReader(), "runnerLifecycleListeners");
        Thread thread = (Thread) threadsHolder.keySet().toArray()[0];
        Assert.assertFalse(thread.isAlive());
        Assert.assertEquals(0, eventListeners.size());
        Assert.assertEquals(0, runnerLifecycleListeners.size());
    }

    @Test
    public void shouldTriggerEventListenerInOrder() throws InterruptedException {
        List<Event> events = new CopyOnWriteArrayList<>();
        CountDownLatch cdl = new CountDownLatch(13);
        getReader().registerEventListener(event -> {
            events.add(event);
            cdl.countDown();
        });
        getReader().doReading();
        cdl.await();

        // Check the event do not lost.
        Assert.assertEquals(13, events.size());
        Assert.assertTrue(events.get(0).getType().equals(EventType.DDL_EVENT) && events.get(0).getOrder() == 0);
        Assert.assertTrue(events.get(1).getType().equals(EventType.DDL_EVENT) && events.get(1).getOrder() == 0);
        Assert.assertTrue(events.get(2).getType().equals(EventType.DDL_EVENT) && events.get(2).getOrder() == 1);
        Assert.assertTrue(events.get(3).getType().equals(EventType.DDL_EVENT) && events.get(3).getOrder() == 2);
        Assert.assertTrue(events.get(4).getType().equals(EventType.ROW_CHANGED_EVENT) && events.get(4).getOrder() == 0);
        Assert.assertTrue(events.get(5).getType().equals(EventType.ROW_CHANGED_EVENT) && events.get(5).getOrder() == 0);
        Assert.assertTrue(events.get(6).getType().equals(EventType.ROW_CHANGED_EVENT) && events.get(6).getOrder() == 1);
        Assert.assertTrue(events.get(7).getType().equals(EventType.ROW_CHANGED_EVENT) && events.get(7).getOrder() == 2);
        Assert.assertTrue(events.get(8).getType().equals(EventType.ROW_CHANGED_EVENT) && events.get(8).getOrder() == 3);
        Assert.assertTrue(events.get(9).getType().equals(EventType.RESOLVED_EVENT) && events.get(9).getOrder() == 0);
        Assert.assertTrue(events.get(10).getType().equals(EventType.RESOLVED_EVENT) && events.get(10).getOrder() == 0);
        Assert.assertTrue(events.get(11).getType().equals(EventType.RESOLVED_EVENT) && events.get(11).getOrder() == 1);
        Assert.assertTrue(events.get(12).getType().equals(EventType.RESOLVED_EVENT) && events.get(12).getOrder() == 2);

        // Check ROW_CHANGED_EVENT ordered
        Assert.assertEquals(5, ((TicdcEventData) events.get(4).getData()).getTicdcEventKey().getTs());
        Assert.assertEquals(6, ((TicdcEventData) events.get(5).getData()).getTicdcEventKey().getTs());
        Assert.assertEquals(7, ((TicdcEventData) events.get(6).getData()).getTicdcEventKey().getTs());
        Assert.assertEquals(8, ((TicdcEventData) events.get(7).getData()).getTicdcEventKey().getTs());
        Assert.assertEquals(9, ((TicdcEventData) events.get(8).getData()).getTicdcEventKey().getTs());
    }

    @Test
    public void shouldCommitHandledOffsets() throws InterruptedException {
        List<Event> events = new CopyOnWriteArrayList<>();
        CountDownLatch cdl = new CountDownLatch(13);
        getReader().registerEventListener(event -> {
            events.add(event);
            if (event.getData() instanceof TicdcEventData) {
                getReader().markTicdcEventAsDone(((TicdcEventData) event.getData()).getTicdcEventValue().getKafkaPartition(),
                        ((TicdcEventData) event.getData()).getTicdcEventValue().getKafkaOffset(), event.getOrder());
                cdl.countDown();
            }
        });
        getReader().doReading();
        cdl.await();
        Thread.sleep(1000L);
        TopicPartition topicPartition = new TopicPartition(SOURCE_TOPIC_NAME, 0);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafka().brokerList());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "reader-" + READER_ID.get());
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Lists.newArrayList(SOURCE_TOPIC_NAME));
        Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(new HashSet<>(Lists.newArrayList(topicPartition)));
        Assert.assertEquals(6, committed.get(topicPartition).offset());
    }

}
