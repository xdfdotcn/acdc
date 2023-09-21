package cn.xdf.acdc.connect.starrocks.sink.streamload.v2;

import cn.xdf.acdc.connect.starrocks.sink.streamload.StreamLoadResponse;
import cn.xdf.acdc.connect.starrocks.sink.streamload.StreamLoadSnapshot;
import cn.xdf.acdc.connect.starrocks.sink.streamload.StreamLoadSnapshot.Transaction;
import cn.xdf.acdc.connect.starrocks.sink.streamload.TableRegion;
import cn.xdf.acdc.connect.starrocks.sink.streamload.TransactionStreamLoader;
import cn.xdf.acdc.connect.starrocks.sink.streamload.properties.StreamLoadTableProperties;

public class MockStreamLoader extends TransactionStreamLoader {
    private MockMode mockMode = MockMode.SUCCESS;
    
    enum MockMode {
        STREAM_LOAD_FAILURE, SUCCESS
    }
    
    public MockStreamLoader(MockMode mockMode) {
        this.mockMode = mockMode;
    }
    
    public MockStreamLoader() {
    }
    
    @Override
    protected boolean doBegin(final TableRegion region) {
        return true;
    }
    
    @Override
    public boolean prepare(final Transaction transaction) {
        return true;
    }
    
    @Override
    protected StreamLoadResponse send(final StreamLoadTableProperties tableProperties, final TableRegion region) {
        for (; null != region.read(); ) {
        }
        
        StreamLoadResponse streamLoadResponse = new StreamLoadResponse();
        
        switch (this.mockMode) {
            case STREAM_LOAD_FAILURE:
                region.callback(new RuntimeException("Mock stream load failure."));
                break;
            default:
                region.complete(streamLoadResponse);
                break;
        }
        
        return streamLoadResponse;
    }
    
    @Override
    public boolean commit(final Transaction transaction) {
        return true;
    }
    
    @Override
    public boolean rollback(final Transaction transaction) {
        return true;
    }
    
    @Override
    public boolean prepare(final StreamLoadSnapshot snapshot) {
        return true;
    }
    
    @Override
    public boolean commit(final StreamLoadSnapshot snapshot) {
        return true;
    }
    
    @Override
    public boolean rollback(final StreamLoadSnapshot snapshot) {
        return true;
    }
}
