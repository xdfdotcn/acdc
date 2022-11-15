package cn.xdf.acdc.connect.core.sink.processor;

import cn.xdf.acdc.connect.core.sink.processor.process.FieldAdditionProcess;
import cn.xdf.acdc.connect.core.sink.processor.process.FieldMappingProcess;
import cn.xdf.acdc.connect.core.sink.processor.process.FieldWhitelistProcess;
import cn.xdf.acdc.connect.core.sink.processor.process.LogicallyDeletionProcess;
import cn.xdf.acdc.connect.core.sink.processor.process.PhysicallyDeletionProcess;
import cn.xdf.acdc.connect.core.sink.processor.process.SinkProcessChain;
import cn.xdf.acdc.connect.core.util.config.DestinationConfig;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.TimeZone;

@Slf4j
public class SinkProcessor implements Processor<SinkRecord> {

    private SinkProcessChain sinkSingleRecordKeyProcessChain = new SinkProcessChain();

    private SinkProcessChain sinkSingleRecordValueProcessChain = new SinkProcessChain();

    private DestinationConfig destinationConfig;

    private TimeZone timeZone;

    public SinkProcessor(final DestinationConfig destinationConfig, final TimeZone timeZone) {
        this.destinationConfig = destinationConfig;
        this.timeZone = timeZone;
        initRecordKeyProcessChain();
        initRecordValueProcessChain();
    }

    private void initRecordValueProcessChain() {
        sinkSingleRecordValueProcessChain
                .addLast(new FieldWhitelistProcess(destinationConfig.getFieldsWhitelist()))
                .addLast(new FieldMappingProcess(destinationConfig.getFieldsMapping()))
                .addLast(new FieldAdditionProcess(destinationConfig.getFieldsToAdd(), timeZone));

        switch (destinationConfig.getDeleteMode()) {
            case PHYSICAL:
                sinkSingleRecordValueProcessChain.addFirst(new PhysicallyDeletionProcess());
                break;
            case LOGICAL:
                sinkSingleRecordValueProcessChain.addLast(new LogicallyDeletionProcess(destinationConfig.getLogicalDeleteFieldName(),
                        destinationConfig.getLogicalDeleteFieldValueDeleted(), destinationConfig.getLogicalDeleteFieldValueNormal()));
                break;
            default:
                break;
        }
    }

    private void initRecordKeyProcessChain() {
        sinkSingleRecordKeyProcessChain.addLast(new FieldMappingProcess(destinationConfig.getFieldsMapping()));
    }

    @Override
    public SinkRecord process(final SinkRecord record) {
        Preconditions.checkArgument(record.value() instanceof Struct);

        boolean isKeyStruct = record.key() instanceof Struct;
        Object recordKey = isKeyStruct ? sinkSingleRecordKeyProcessChain.process((Struct) record.key()) : record.key();
        Schema recordKeySchema = isKeyStruct ? ((Struct) recordKey).schema() : record.keySchema();
        Struct recordValue = sinkSingleRecordValueProcessChain.process((Struct) record.value());
        Schema recordValueSchema = recordValue == null ? null : recordValue.schema();
        return new SinkRecord(record.topic(), record.kafkaPartition(), recordKeySchema, recordKey, recordValueSchema,
                recordValue, record.kafkaOffset(), record.timestamp(), record.timestampType(), record.headers());
    }

}
