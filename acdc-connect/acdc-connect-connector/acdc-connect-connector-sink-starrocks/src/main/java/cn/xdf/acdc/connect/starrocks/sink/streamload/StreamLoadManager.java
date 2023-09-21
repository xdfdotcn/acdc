package cn.xdf.acdc.connect.starrocks.sink.streamload;

import cn.xdf.acdc.connect.starrocks.sink.SinkData;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;

public interface StreamLoadManager {
    
    void init();
    
    void write(String database, String table, SinkData<byte[]> sinkData);
    
    void write(Collection<SinkRecord> records);
    
    void callback(StreamLoadResponse response);
    
    void callback(Throwable e);
    
    StreamLoadSnapshot snapshot();
    
    boolean prepare(StreamLoadSnapshot snapshot);
    
    boolean commit(StreamLoadSnapshot snapshot);
    
    boolean abort(StreamLoadSnapshot snapshot);
    
    boolean abort();
    
    void close();
}
