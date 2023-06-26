package cn.xdf.acdc.connect.kafka.sink;

import cn.xdf.acdc.connect.core.sink.kafka.RecordConverter;
import cn.xdf.acdc.connect.kafka.sink.utils.KafkaSinkTestUtil;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

@RunWith(MockitoJUnitRunner.class)
public class KafkaWriterTest {

    @Mock
    private Converter keyConverter;

    @Mock
    private Converter valueConverter;

    @Mock
    private XdfRecordConverter xdfRecordConverter;

    @Mock
    private KafkaProducer<byte[], byte[]> producer;

    private KafkaWriter kafkaWriter;

    @Before
    public void setup() {
        kafkaWriter = new KafkaWriter(getKafkaSinkConfig(), keyConverter, valueConverter, producer, 3, 1000);
    }

    @Test
    public void testConverterShouldCallDifMethodWithDifType() {
        kafkaWriter.doWrite(producer, "target", fakeSinkRecords(1, 0, 0).get(0));
        Mockito.verify(keyConverter).fromConnectData(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
        Mockito.verify(valueConverter).fromConnectData(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());

        kafkaWriter = new KafkaWriter(getKafkaSinkConfig(), xdfRecordConverter, xdfRecordConverter, producer, 3, 1000);
        kafkaWriter.doWrite(producer, "target", fakeSinkRecords(1, 0, 0).get(0));
        ArgumentCaptor<List<String>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(xdfRecordConverter).fromRecordData(ArgumentMatchers.any(), argumentCaptor.capture(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
        List<String> expect = new ArrayList<>();
        expect.add("id");
        Assert.assertEquals(expect, argumentCaptor.getValue());
    }

    @Test(expected = ConnectException.class)
    public void testDoWriteShouldThrowExceptionsWithFullQueue() {
        fakeSinkRecords(1001, 0, 0).forEach(sinkRecord -> {
            kafkaWriter.doWrite(producer, "target", sinkRecord);
        });
    }

    @Test
    public void testDoWriteShouldUpdateOffsetWithRecordSent() throws NoSuchFieldException, IllegalAccessException {
        fakeSinkRecords(1001, 0, 0).forEach(sinkRecord -> {
            kafkaWriter.doWrite(producer, "target", sinkRecord);
            ArgumentCaptor<Callback> argumentCaptor = ArgumentCaptor.forClass(Callback.class);
            if (sinkRecord.kafkaOffset() == 0) {
                Mockito.verify(producer).send(ArgumentMatchers.any(), argumentCaptor.capture());
                argumentCaptor.getValue().onCompletion(null, null);
            }
        });

        Map<TopicPartition, Long> expectOffset = new HashMap<>();
        expectOffset.put(new TopicPartition("target", 0), 0L);
        Assert.assertEquals(expectOffset, kafkaWriter.getCurrentOffsets());

        Map<TopicPartition, OffsetAndMetadata> expectCommitOffset = new HashMap<>();
        expectCommitOffset.put(new TopicPartition("target", 0), new OffsetAndMetadata(1L));
        Assert.assertEquals(expectCommitOffset, kafkaWriter.getToBeCommittedOffsets());

        Map<TopicPartition, Queue<RecordForwardResult>> queueMap = kafkaWriter.getRecordForwardResultQueues();
        Assert.assertEquals(1000, queueMap.values().stream().findFirst().get().size());
    }

    @Test
    public void testDoWriteShouldCleanQueueWithAllRecordSent() throws NoSuchFieldException, IllegalAccessException {
        fakeSinkRecords(1000, 0, 0).forEach(sinkRecord -> {
            kafkaWriter.doWrite(producer, "target", sinkRecord);
        });
        ArgumentCaptor<Callback> argumentCaptor = ArgumentCaptor.forClass(Callback.class);
        Mockito.verify(producer, Mockito.times(1000)).send(ArgumentMatchers.any(), argumentCaptor.capture());

        argumentCaptor.getAllValues().forEach(callback -> callback.onCompletion(null, null));

        Map<TopicPartition, OffsetAndMetadata> expectCommitOffset = new HashMap<>();
        expectCommitOffset.put(new TopicPartition("target", 0), new OffsetAndMetadata(1000L));
        Assert.assertEquals(expectCommitOffset, kafkaWriter.getToBeCommittedOffsets());

        Map<TopicPartition, Queue<RecordForwardResult>> queueMap = kafkaWriter.getRecordForwardResultQueues();
        Assert.assertEquals(0, queueMap.values().stream().findFirst().get().size());
    }

    @Test(expected = ConnectException.class)
    public void testDoWriteShouldThrowExceptionsWithRecordSentErrors() {
        fakeSinkRecords(1000, 1, 10000).forEach(sinkRecord -> {
            kafkaWriter.doWrite(producer, "target", sinkRecord);
            ArgumentCaptor<Callback> argumentCaptor = ArgumentCaptor.forClass(Callback.class);
            if (sinkRecord.kafkaOffset() == 10000) {
                Mockito.verify(producer).send(ArgumentMatchers.any(), argumentCaptor.capture());
                argumentCaptor.getValue().onCompletion(null, new RuntimeException("mock record send exception"));
            }
        });
    }

    private List<SinkRecord> fakeSinkRecords(final int num, final int partition, final int beginOffset) {
        List<SinkRecord> result = new ArrayList<>();
        Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .build();
        Schema valueSchema = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .build();
        for (long i = 0; i < num; i++) {
            Struct key = new Struct(keySchema);
            key.put("id", i);

            Struct value = new Struct(valueSchema);
            value.put("id", i);
            value.put("name", "record-" + i);
            value.put("age", 10);
            SinkRecord sinkRecord = new SinkRecord("target", partition, keySchema, key, valueSchema, value, i + beginOffset);
            result.add(sinkRecord);
        }
        return result;
    }

    private KafkaSinkConfig getKafkaSinkConfig() {
        return new KafkaSinkConfig(KafkaSinkTestUtil.fakeKafkaSinkTaskConfig());
    }

    interface XdfRecordConverter extends Converter, RecordConverter {

    }
}
