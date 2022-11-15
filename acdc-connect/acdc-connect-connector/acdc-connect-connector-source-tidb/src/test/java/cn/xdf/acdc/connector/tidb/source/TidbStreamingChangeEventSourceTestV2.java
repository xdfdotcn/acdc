package cn.xdf.acdc.connector.tidb.source;

import cn.xdf.acdc.connector.tidb.reader.Event;
import cn.xdf.acdc.connector.tidb.reader.EventType;
import cn.xdf.acdc.connector.tidb.reader.TidbDataReader;
import com.pingcap.ticdc.cdc.KafkaMessage;
import com.pingcap.ticdc.cdc.TicdcEventData;
import com.pingcap.ticdc.cdc.key.TicdcEventKey;
import com.pingcap.ticdc.cdc.value.TicdcEventDDL;
import com.pingcap.ticdc.cdc.value.TicdcEventResolve;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RunWith(PowerMockRunner.class)
@PrepareForTest(TidbStreamingChangeEventSource.class)
@PowerMockIgnore("javax.management.*")
public class TidbStreamingChangeEventSourceTestV2 {

    private TidbStreamingChangeEventSource streamingChangeEventSource;

    @Mock
    private EventDispatcher<TableId> dispatcher;

    @Mock
    private ErrorHandler errorHandler;

    @Mock
    private Clock clock;

    @Mock
    private TidbSourceTaskContext tidbSourceTaskContext;

    @Mock
    private TidbStreamingChangeEventSourceMetrics metrics;

    @Mock
    private ChangeEventSource.ChangeEventSourceContext sourceContext;

    @Mock
    private TidbDataReader reader;

    @Mock
    private TidbDatabaseSchema schema;

    @Mock
    private TidbOffsetContext offsetContext;

    @Before
    public void setUp() {
        streamingChangeEventSource = new TidbStreamingChangeEventSource(dispatcher, errorHandler, clock, schema, tidbSourceTaskContext, reader, metrics);
    }

    @Test
    public void testExecuteShouldFillEventHandlersWith4Type() throws InterruptedException, IllegalAccessException {
        initExecute();
        PowerMock.replayAll();
        streamingChangeEventSource.execute(sourceContext);
        Assert.assertEquals(4, getEventTypeMap().size());
    }

    @Test
    public void testHandleRunnerStopEventShouldGetThrowableIfPresent() throws InterruptedException, IllegalAccessException {
        initExecute();
        Capture<Throwable> throwableCapture = EasyMock.newCapture();
        errorHandler.setProducerThrowable(EasyMock.capture(throwableCapture));
        PowerMock.replayAll();
        streamingChangeEventSource.execute(sourceContext);
        EnumMap<EventType, BlockingConsumer<Event>> eventMap = getEventTypeMap();
        Exception e = new Exception();
        Event endRunnerEvent = new Event(EventType.RUNNER_STOP_EVENT, e);
        eventMap.get(EventType.RUNNER_STOP_EVENT).accept(endRunnerEvent);
        Assert.assertEquals(e, throwableCapture.getValue());
    }

    @Test
    public void testHandleRunnerStopEventShouldGetRetryableExceptionIfNotPresent() throws InterruptedException, IllegalAccessException {
        initExecute();
        Capture<Throwable> throwableCapture = EasyMock.newCapture();
        errorHandler.setProducerThrowable(EasyMock.capture(throwableCapture));
        PowerMock.replayAll();
        streamingChangeEventSource.execute(sourceContext);
        EnumMap<EventType, BlockingConsumer<Event>> eventMap = getEventTypeMap();
        Event endRunnerEvent = new Event(EventType.RUNNER_STOP_EVENT, null);
        eventMap.get(EventType.RUNNER_STOP_EVENT).accept(endRunnerEvent);
        Assert.assertTrue(throwableCapture.getValue() instanceof RetriableException);
    }

    @Test(expected = ConnectException.class)
    public void testHandleResolvedEventShouldThrowConnectExceptionIfEventTypeNotMatch() throws InterruptedException, IllegalAccessException {
        initExecute();
        PowerMock.replayAll();
        streamingChangeEventSource.execute(sourceContext);
        EnumMap<EventType, BlockingConsumer<Event>> eventMap = getEventTypeMap();
        Event endRunnerEvent = new Event(EventType.RESOLVED_EVENT, null);
        eventMap.get(EventType.RESOLVED_EVENT).accept(endRunnerEvent);
    }

    @Test
    public void testHandleResolvedEventShouldCommitOffsetAndSetContextOffsetAsExpect() throws InterruptedException, IllegalAccessException {
        initExecute();
        tidbSourceTaskContext.getOffsetContext();
        PowerMock.expectLastCall().andReturn(offsetContext);
        Capture<Map> mapCapture = EasyMock.newCapture();
        offsetContext.setOffset(EasyMock.capture(mapCapture));
        PowerMock.expectLastCall();
        Capture<Integer> partitionCapture = EasyMock.newCapture();
        Capture<Long> offsetCapture = EasyMock.newCapture();
        Capture<Integer> eventOderCapture = EasyMock.newCapture();
        reader.markTicdcEventAsDone(EasyMock.captureInt(partitionCapture), EasyMock.captureLong(offsetCapture), EasyMock.captureInt(eventOderCapture));
        PowerMock.expectLastCall();
        PowerMock.replayAll();
        streamingChangeEventSource.execute(sourceContext);
        EnumMap<EventType, BlockingConsumer<Event>> eventMap = getEventTypeMap();
        TicdcEventKey key = new TicdcEventKey();
        KafkaMessage kafkaMessage = new KafkaMessage();
        long offset = 6L;
        int partition = 2;
        kafkaMessage.setOffset(offset);
        kafkaMessage.setPartition(partition);
        TicdcEventResolve value = new TicdcEventResolve(kafkaMessage);
        TicdcEventData ticdcEventData = new TicdcEventData(key, value);
        Event endRunnerEvent = new Event(EventType.RESOLVED_EVENT, ticdcEventData);
        eventMap.get(EventType.RESOLVED_EVENT).accept(endRunnerEvent);
        Map<String, Object> expect = new HashMap() {
            {
                put(TidbOffsetContext.READER_PARTITION, partition);
                put(TidbOffsetContext.READER_OFFSET, offset);
                put(TidbOffsetContext.READER_EVENT_ORDER_IN_BATCH, 0);
                put(TidbOffsetContext.READER_EVENT_TYPE, EventType.RESOLVED_EVENT.getDesc());
            }
        };
        Assert.assertEquals(expect, mapCapture.getValue());
        Assert.assertEquals(Integer.valueOf(partition), partitionCapture.getValue());
        Assert.assertEquals(Long.valueOf(offset), offsetCapture.getValue());
        Assert.assertEquals(Integer.valueOf(0), eventOderCapture.getValue());
    }

    @Test(expected = ConnectException.class)
    public void testHandleDdlEventShouldThrowConnectExceptionIfEventTypeNotMatch() throws InterruptedException, IllegalAccessException {
        initExecute();
        PowerMock.replayAll();
        streamingChangeEventSource.execute(sourceContext);
        EnumMap<EventType, BlockingConsumer<Event>> eventMap = getEventTypeMap();
        Event endRunnerEvent = new Event(EventType.DDL_EVENT, null);
        eventMap.get(EventType.DDL_EVENT).accept(endRunnerEvent);
    }

    @Test
    public void testHandleDdlEventShouldCommitOffsetAndSetContextOffsetAsExpect() throws InterruptedException, IllegalAccessException {
        initExecute();
        tidbSourceTaskContext.getOffsetContext();
        PowerMock.expectLastCall().andReturn(offsetContext);
        Capture<Map> mapCapture = EasyMock.newCapture();
        offsetContext.setOffset(EasyMock.capture(mapCapture));
        PowerMock.expectLastCall();
        Capture<Integer> partitionCapture = EasyMock.newCapture();
        Capture<Long> offsetCapture = EasyMock.newCapture();
        Capture<Integer> eventOderCapture = EasyMock.newCapture();
        reader.markTicdcEventAsDone(EasyMock.captureInt(partitionCapture), EasyMock.captureLong(offsetCapture), EasyMock.captureInt(eventOderCapture));
        PowerMock.expectLastCall();
        PowerMock.replayAll();
        streamingChangeEventSource.execute(sourceContext);
        EnumMap<EventType, BlockingConsumer<Event>> eventMap = getEventTypeMap();
        TicdcEventKey key = new TicdcEventKey();
        KafkaMessage kafkaMessage = new KafkaMessage();
        long offset = 6L;
        int partition = 2;
        kafkaMessage.setOffset(offset);
        kafkaMessage.setPartition(partition);
        TicdcEventDDL value = new TicdcEventDDL(kafkaMessage);
        TicdcEventData ticdcEventData = new TicdcEventData(key, value);
        Event endRunnerEvent = new Event(EventType.DDL_EVENT, ticdcEventData);
        eventMap.get(EventType.DDL_EVENT).accept(endRunnerEvent);
        Map<String, Object> expect = new HashMap() {
            {
                put(TidbOffsetContext.READER_PARTITION, partition);
                put(TidbOffsetContext.READER_OFFSET, offset);
                put(TidbOffsetContext.READER_EVENT_ORDER_IN_BATCH, 0);
                put(TidbOffsetContext.READER_EVENT_TYPE, EventType.DDL_EVENT.getDesc());
            }
        };
        Assert.assertEquals(expect, mapCapture.getValue());
        Assert.assertEquals(Integer.valueOf(partition), partitionCapture.getValue());
        Assert.assertEquals(Long.valueOf(offset), offsetCapture.getValue());
        Assert.assertEquals(Integer.valueOf(0), eventOderCapture.getValue());
    }

    private EnumMap<EventType, BlockingConsumer<Event>> getEventTypeMap() throws IllegalAccessException {
        Field eventHandlers = PowerMockito.field(TidbStreamingChangeEventSource.class, "eventHandlers");
        return (EnumMap) eventHandlers.get(streamingChangeEventSource);
    }

    private void initExecute() throws InterruptedException {
        reader.registerEventListener(EasyMock.anyObject());
        PowerMock.expectLastCall();
        reader.doReading();
        PowerMock.expectLastCall();
        sourceContext.isRunning();
        PowerMock.expectLastCall().andReturn(false);
        reader.close();
        PowerMock.expectLastCall();
    }
}
