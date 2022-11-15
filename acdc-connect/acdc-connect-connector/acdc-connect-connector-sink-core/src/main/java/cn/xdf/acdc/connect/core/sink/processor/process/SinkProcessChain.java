package cn.xdf.acdc.connect.core.sink.processor.process;

import cn.xdf.acdc.connect.core.sink.data.TemporaryFieldAndValue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

@Slf4j
public class SinkProcessChain implements ProcessChain {

    private Deque<Process> processes = new LinkedList<>();

    private Map<Set<String>, Schema> schemaCache = new HashMap<>();

    private Schema currentValueSchema;

    /**
     * Add a process the head of chain.
     *
     * @param process The process to be added
     * @return the chain itself
     */
    public SinkProcessChain addFirst(final Process process) {
        processes.addFirst(process);
        return this;
    }

    /**
     * Add a process the tail of chain.
     *
     * @param process The process to be added
     * @return the chain itself
     */
    public SinkProcessChain addLast(final Process process) {
        processes.addLast(process);
        return this;
    }

    @Override
    public Struct process(final Struct struct) {
        maybeNewSchema(struct.schema());

        Map<String, TemporaryFieldAndValue> fieldAndValues = parseToFieldAndValues(struct);
        executeProcessChain(fieldAndValues);

        return newStruct(fieldAndValues);
    }

    private void executeProcessChain(final Map<String, TemporaryFieldAndValue> fieldAndValues) {
        log.debug("Before process chain fields and values {}", fieldAndValues);
        for (Process each : processes) {
            if (!each.execute(fieldAndValues)) {
                return;
            }
        }
        log.debug("After process chain fields and values {}", fieldAndValues);
    }

    private Struct newStruct(final Map<String, TemporaryFieldAndValue> fieldAndValues) {
        if (fieldAndValues.isEmpty()) {
            return null;
        }

        Schema schema = getCorrespondingSchema(fieldAndValues);
        Struct recordValue = new Struct(schema);

        for (int i = 0; i < schema.fields().size(); i++) {
            Field each = schema.fields().get(i);
            recordValue.put(new Field(each.name(), i, each.schema()), fieldAndValues.get(each.name()).getValue());
        }

        return recordValue;
    }

    private Schema getCorrespondingSchema(final Map<String, TemporaryFieldAndValue> fieldAndValues) {
        return schemaCache.computeIfAbsent(fieldAndValues.keySet(), key -> {
            // todo: 同样的 name 和 version 向后传递是否会有问题？
            SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(currentValueSchema.name()).version(currentValueSchema.version());
            fieldAndValues.values().forEach(each -> {
                schemaBuilder.field(each.getName(), each.getSchema());
            });
            return schemaBuilder.build();
        });
    }

    private void maybeNewSchema(final Schema valueSchema) {
        if (!valueSchema.equals(currentValueSchema)) {
            schemaCache.clear();
            currentValueSchema = valueSchema;
        }
    }

    private Map<String, TemporaryFieldAndValue> parseToFieldAndValues(final Struct recordValue) {
        Map<String, TemporaryFieldAndValue> fieldAndValues = new HashMap<>();
        for (Field each : recordValue.schema().fields()) {
            fieldAndValues.put(each.name(), new TemporaryFieldAndValue(each.schema(), each.name(), recordValue.get(each.name())));
        }
        return fieldAndValues;
    }

}
