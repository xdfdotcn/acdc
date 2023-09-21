package cn.xdf.acdc.connect.starrocks.sink.streamload;


import cn.xdf.acdc.connect.starrocks.sink.streamload.properties.StreamLoadProperties;

import java.util.concurrent.Future;

public interface StreamLoader {
    
    void start(StreamLoadProperties properties, StreamLoadManager manager);
    
    void close();
    
    boolean begin(TableRegion region);
    
    Future<StreamLoadResponse> send(TableRegion region);
    
    boolean prepare(StreamLoadSnapshot.Transaction transaction);
    
    boolean commit(StreamLoadSnapshot.Transaction transaction);
    
    boolean rollback(StreamLoadSnapshot.Transaction transaction);
    
    boolean prepare(StreamLoadSnapshot snapshot);
    
    boolean commit(StreamLoadSnapshot snapshot);
    
    boolean rollback(StreamLoadSnapshot snapshot);
}
