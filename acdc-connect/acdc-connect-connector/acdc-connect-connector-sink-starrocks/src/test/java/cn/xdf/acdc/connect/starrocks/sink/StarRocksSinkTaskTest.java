package cn.xdf.acdc.connect.starrocks.sink;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import cn.xdf.acdc.connect.starrocks.sink.config.StarRocksSinkConfig;
import cn.xdf.acdc.connect.starrocks.sink.streamload.StreamLoadManager;

@RunWith(MockitoJUnitRunner.class)
public class StarRocksSinkTaskTest {
    
    private StreamLoadManager streamLoadManager;
    
    private StarRocksSinkConfig starRocksSinkConfig;
    
    private OffsetTracker offsetTracker;
    
    @Before
    public void setUp() {
        streamLoadManager = Mockito.mock(StreamLoadManager.class);
        starRocksSinkConfig = Mockito.mock(StarRocksSinkConfig.class);
        offsetTracker = Mockito.mock(OffsetTracker.class);
    }
    
    @Test
    public void testPreCommit() {
        StarRocksSinkTask sinkTask = new StarRocksSinkTask(
                this.starRocksSinkConfig,
                this.offsetTracker,
                this.streamLoadManager
        );
        
        sinkTask.preCommit(null);
        
        Mockito.verify(offsetTracker, Mockito.times(1)).offsets();
    }
    
    @Test
    public void testClose() {
        StarRocksSinkTask sinkTask = new StarRocksSinkTask(
                this.starRocksSinkConfig,
                this.offsetTracker,
                this.streamLoadManager
        );
        
        sinkTask.close(null);
        
        Mockito.verify(offsetTracker, Mockito.times(1)).close(Mockito.any());
    }
    
    @Test
    public void testStop() {
        StarRocksSinkTask sinkTask = new StarRocksSinkTask(
                this.starRocksSinkConfig,
                this.offsetTracker,
                this.streamLoadManager
        );
        
        sinkTask.stop();
        
        Mockito.verify(streamLoadManager, Mockito.times(1)).abort();
        Mockito.verify(streamLoadManager, Mockito.times(1)).close();
    }
}
