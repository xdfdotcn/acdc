package cn.xdf.acdc.connect.starrocks.sink.streamload;


import cn.xdf.acdc.connect.starrocks.sink.SinkData;
import cn.xdf.acdc.connect.starrocks.sink.streamload.http.StreamLoadEntityMeta;
import cn.xdf.acdc.connect.starrocks.sink.streamload.properties.StreamLoadTableProperties;

import java.util.concurrent.Future;

public interface TableRegion {
    
    StreamLoadTableProperties getProperties();
    
    String getUniqueKey();
    
    String getDatabase();
    
    String getTable();
    
    void setLabel(String label);
    
    String getLabel();
    
    long getCacheBytes();
    
    long getFlushBytes();
    
    StreamLoadEntityMeta getEntityMeta();
    
    long getLastWriteTimeMillis();
    
    void resetAge();
    
    long getAndIncrementAge();
    
    long getAge();
    
    int write(SinkData<byte[]> data);
    
    byte[] read();
    
    boolean testPrepare();
    
    boolean prepare();
    
    boolean flush();
    
    boolean cancel();
    
    void callback(StreamLoadResponse response);
    
    void callback(Throwable e);
    
    void complete(StreamLoadResponse response);
    
    void setResult(Future<?> result);
    
    Future<?> getResult();
    
    boolean isReadable();
    
    boolean isFlushing();
}
