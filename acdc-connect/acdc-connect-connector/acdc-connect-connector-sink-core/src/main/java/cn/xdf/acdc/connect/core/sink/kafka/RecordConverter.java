package cn.xdf.acdc.connect.core.sink.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.List;

public interface RecordConverter {

    /**
     * Convert from Kafka Connect data object to native object for serialization and support update key.
     * In some cases or during to old data format compatibility, key need to regenerated according to custom config.
     *
     * @param topic the topic associated with the data
     * @param keySet new key set
     * @param valueSchema the schema for the kafka record value
     * @param value kafka record value struct
     * @param order record order
     * @return the serialized data
     */
    ProducerRecord<byte[], byte[]> fromRecordData(String topic, List<String> keySet, Schema valueSchema, Struct value, Long order);
}
