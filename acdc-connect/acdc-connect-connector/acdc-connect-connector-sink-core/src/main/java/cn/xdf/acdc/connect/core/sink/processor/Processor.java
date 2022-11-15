package cn.xdf.acdc.connect.core.sink.processor;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.sink.SinkRecord;

public interface Processor<T extends ConnectRecord<T>> {

    /**
     * Do process for connect record.
     *
     * @param record The record to be processed
     * @return sink record after processes
     */
    SinkRecord process(T record);

}
