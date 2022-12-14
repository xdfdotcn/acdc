package cn.xdf.acdc.devops.scheduler;

import cn.xdf.acdc.devops.core.domain.entity.ConnectClusterDO;
import cn.xdf.acdc.devops.service.entity.ConnectClusterService;
import cn.xdf.acdc.devops.statemachine.ConnectorStateHandler;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.scheduling.TaskScheduler;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

@RunWith(MockitoJUnitRunner.class)
public class ConnectorStateSchedulerTest {

    @Mock
    private TaskScheduler taskScheduler;

    @Mock
    private ConnectClusterService connectClusterService;

    @Mock
    private ConnectorStateHandler connectorStateHandler;

    @Mock
    private ScheduledFuture scheduledFuture;

    private ConnectorStateScheduler scheduler;

    @Before
    public void setup() {
        scheduler = new ConnectorStateScheduler(taskScheduler, connectClusterService, connectorStateHandler);
    }

    @Test
    public void testRefreshSchedulerTasksShouldAddNewSchedulerTasks() {
        ConnectClusterDO connectCluster1 = ConnectClusterDO.builder().id(1L).connectRestApiUrl("test1:8083").build();
        Mockito.when(connectClusterService.findAll()).thenReturn(Lists.newArrayList(connectCluster1));
        Mockito.when(connectorStateHandler.getUserTriggerEventHandlers()).thenReturn(new HashMap<>());
        Mockito.when(taskScheduler.scheduleAtFixedRate(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(scheduledFuture);
        scheduler.refreshSchedulerTasks();
        Mockito.verify(taskScheduler, Mockito.times(2)).scheduleAtFixedRate(ArgumentMatchers.any(), ArgumentMatchers.any());
        ConnectClusterDO connectCluster2 = ConnectClusterDO.builder().id(2L).connectRestApiUrl("test2:8083").build();
        Mockito.when(connectClusterService.findAll()).thenReturn(Lists.newArrayList(connectCluster1, connectCluster2));
        Mockito.when(connectorStateHandler.getUserTriggerEventHandlers()).thenReturn(new HashMap<>());
        Mockito.when(taskScheduler.scheduleAtFixedRate(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(scheduledFuture);
        scheduler.refreshSchedulerTasks();
        Mockito.verify(taskScheduler, Mockito.times(4)).scheduleAtFixedRate(ArgumentMatchers.any(), ArgumentMatchers.any());
    }

    @Test
    public void testRefreshSchedulerTasksShouldRemoveSchedulerTasksWhichNotExists() {
        testRefreshSchedulerTasksShouldAddNewSchedulerTasks();
        ConnectClusterDO connectCluster2 = ConnectClusterDO.builder().id(2L).connectRestApiUrl("test2:8083").build();
        List<ConnectClusterDO> connectClusters = Lists.newArrayList(connectCluster2, connectCluster2);
        Mockito.when(connectClusterService.findAll()).thenReturn(connectClusters);
        scheduler.refreshSchedulerTasks();
        Mockito.verify(scheduledFuture, Mockito.times(2)).cancel(false);
    }

}
