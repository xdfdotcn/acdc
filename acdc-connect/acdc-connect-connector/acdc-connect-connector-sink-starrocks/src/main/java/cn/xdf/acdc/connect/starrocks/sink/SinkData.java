package cn.xdf.acdc.connect.starrocks.sink;

import org.apache.kafka.connect.sink.SinkRecord;

public class SinkData<T> {
    
    private SinkRecord sinkRecord;
    
    private T data;
    
    public SinkData(final SinkRecord sinkRecord, final T data) {
        this.sinkRecord = sinkRecord;
        this.data = data;
    }
    
    /**
     * Get the sink record.
     *
     * @return sink record
     */
    public SinkRecord getSinkRecord() {
        return sinkRecord;
    }
    
    /**
     * Get the data.
     *
     * @return data
     */
    public T getData() {
        return data;
    }
}
