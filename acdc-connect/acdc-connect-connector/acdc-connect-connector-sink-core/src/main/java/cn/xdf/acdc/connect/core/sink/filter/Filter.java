package cn.xdf.acdc.connect.core.sink.filter;

import org.apache.kafka.connect.sink.SinkRecord;

public interface Filter {

    /**
     * If the record is filtered out return false, else return true.
     *
     * @param sinkRecord sink record
     * @return filter result
     */
    boolean filter(SinkRecord sinkRecord);

}


