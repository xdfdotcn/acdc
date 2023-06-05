package cn.xdf.acdc.devops.controller;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.query.ConnectionQuery;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorService;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.DefaultManagedTaskScheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

@RunWith(MockitoJUnitRunner.class)
public class ConnectionControllerTest {
    
    @Mock
    private ConnectionService connectionService;
    
    @Mock
    private ConnectorService connectorService;
    
    private ConnectionController connectionController;
    
    private volatile int scheduleTimes = 1;
    
    private final TaskScheduler taskScheduler = fakeTaskScheduler();
    
    @Before
    public void setup() {
        connectionController = new ConnectionController(taskScheduler, connectionService, connectorService);
    }
    
    private TaskScheduler fakeTaskScheduler() {
        return new DefaultManagedTaskScheduler() {
            @Override
            public ScheduledFuture<?> scheduleAtFixedRate(final Runnable task, final Duration period) {
                int localScheduleTimes = scheduleTimes;
                while (localScheduleTimes-- > 0) {
                    task.run();
                }
                return null;
            }
        };
    }
    
    @Test
    public void testConnectionToConnectorShouldCreateConnectorWithNewConnection() {
        List<ConnectionDTO> connectionDTOS = fakeNewConnectionDtoList();
        Mockito.when(connectionService.query(ArgumentMatchers.any())).thenReturn(connectionDTOS);
        Mockito.when(connectionService.applyConnectionToConnector(ArgumentMatchers.any()))
                .thenReturn(fakeReturnConnectionDtoList().get(0))
                .thenReturn(fakeReturnConnectionDtoList().get(1));
        
        connectionController.start();
        ArgumentCaptor<Long> connectionDtoIdCaptor = ArgumentCaptor.forClass(Long.class);
        Mockito.verify(connectionService, Mockito.times(1)).applyConnectionToConnector(connectionDtoIdCaptor.capture());
        
        ConnectionDTO runningConnection = connectionDTOS.get(0);
        Assert.assertEquals(runningConnection.getId(), connectionDtoIdCaptor.getValue());
    }
    
    @Test
    public void testQueryShouldUpdateBeginQueryTime() {
        Mockito.when(connectionService.query(ArgumentMatchers.any())).thenReturn(fakeNewConnectionDtoList());
        Mockito.when(connectionService.applyConnectionToConnector(ArgumentMatchers.any()))
                .thenReturn(fakeReturnConnectionDtoList().get(0))
                .thenReturn(fakeReturnConnectionDtoList().get(1));
        
        scheduleTimes = 2;
        connectionController.start();
        ArgumentCaptor<ConnectionQuery> connectionQueryCaptor = ArgumentCaptor.forClass(ConnectionQuery.class);
        Mockito.verify(connectionService, Mockito.times(2)).query(connectionQueryCaptor.capture());
        Assert.assertEquals(
                Lists.newArrayList(
                        new ConnectionQuery().setBeginUpdateTime(new Date(Long.MIN_VALUE)).setRequisitionState(RequisitionState.APPROVED),
                        new ConnectionQuery().setBeginUpdateTime(Date.from(Instant.parse("2022-06-06T10:25:30Z"))).setRequisitionState(RequisitionState.APPROVED)
                ),
                connectionQueryCaptor.getAllValues());
    }
    
    @Test
    public void testConnectionToConnectorShouldUpdateConnectorWithInitializedUpdatedConnection() {
        List<ConnectionDTO> fakeConnectionDTOList = fakeInitializedConnectionDtoList();
        
        Mockito.when(connectionService.query(ArgumentMatchers.any())).thenReturn(fakeConnectionDTOList);
        
        connectionController.start();
        
        ArgumentCaptor<Long> connectorIds = ArgumentCaptor.forClass(Long.class);
        Mockito.verify(connectorService, Mockito.times(1)).stop(connectorIds.capture());
        Mockito.verify(connectorService, Mockito.times(1)).start(connectorIds.capture());
        Assert.assertEquals(Lists.newArrayList(2001L, 2002L), connectorIds.getAllValues());
        
        ArgumentCaptor<Long> connectionIds = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectionState> connectionActualStates = ArgumentCaptor.forClass(ConnectionState.class);
        Mockito.verify(connectionService, Mockito.times(2)).updateActualState(connectionIds.capture(), connectionActualStates.capture());
        Assert.assertEquals(Lists.newArrayList(1L, 2L), connectionIds.getAllValues());
        Assert.assertEquals(Lists.newArrayList(ConnectionState.STOPPING, ConnectionState.STARTING), connectionActualStates.getAllValues());
    }
    
    @Test
    public void testConnectionToConnectorShouldUpdateConnectorWithUpdatedConnection() {
        List<ConnectionDTO> fakeConnectionDtoList = fakeInitializedUpdatedConnectionDtoList();
        
        Mockito.when(connectionService.query(ArgumentMatchers.any()))
                .thenReturn(fakeNewConnectionDtoList())
                .thenReturn(fakeConnectionDtoList);
        
        Mockito.when(connectionService.applyConnectionToConnector(ArgumentMatchers.any()))
                .thenReturn(fakeReturnConnectionDtoList().get(0))
                .thenReturn(fakeReturnConnectionDtoList().get(1));
        
        scheduleTimes = 2;
        connectionController.start();
        ArgumentCaptor<Long> connectorIds = ArgumentCaptor.forClass(Long.class);
        Mockito.verify(connectorService, Mockito.times(1)).start(connectorIds.capture());
        Mockito.verify(connectorService, Mockito.times(1)).stop(connectorIds.capture());
        Assert.assertEquals(Lists.newArrayList(2002L, 2001L), connectorIds.getAllValues());
        
        ArgumentCaptor<Long> connectionIds = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectionState> connectionActualStates = ArgumentCaptor.forClass(ConnectionState.class);
        Mockito.verify(connectionService, Mockito.times(3)).updateActualState(connectionIds.capture(), connectionActualStates.capture());
        Assert.assertEquals(Lists.newArrayList(1L, 1L, 2L), connectionIds.getAllValues());
        Assert.assertEquals(Lists.newArrayList(ConnectionState.STARTING, ConnectionState.STOPPING, ConnectionState.STARTING), connectionActualStates.getAllValues());
    }
    
    @Test
    public void testConnectorToConnectionShouldIgnoreWithIncompleteConnectors() {
        Mockito.when(connectionService.query(ArgumentMatchers.any())).thenReturn(fakeInitializedConnectionDtoList());
        Mockito.when(connectorService.query(ArgumentMatchers.any())).thenReturn(fakeIncompleteConnectorDtoList());
        
        connectionController.start();
        
        ArgumentCaptor<ConnectionState> connectionActualStates = ArgumentCaptor.forClass(ConnectionState.class);
        Mockito.verify(connectionService, Mockito.times(2)).updateActualState(ArgumentMatchers.any(), connectionActualStates.capture());
        Assert.assertEquals(Lists.newArrayList(ConnectionState.STOPPING, ConnectionState.STARTING), connectionActualStates.getAllValues());
    }
    
    @Test
    public void testConnectorToConnectionShouldUpdateConnectionActualStateWithUpdatedConnectorState() {
        Mockito.when(connectionService.query(ArgumentMatchers.any())).thenReturn(fakeInitializedConnectionDtoList());
        Mockito.when(connectorService.query(ArgumentMatchers.any())).thenReturn(fakeConnectorDtoList());
        
        connectionController.start();
        
        ArgumentCaptor<ConnectionState> connectionActualStates = ArgumentCaptor.forClass(ConnectionState.class);
        Mockito.verify(connectionService, Mockito.times(4)).updateActualState(ArgumentMatchers.any(), connectionActualStates.capture());
        Assert.assertEquals(Lists.newArrayList(ConnectionState.STOPPING, ConnectionState.STARTING, ConnectionState.STOPPED, ConnectionState.RUNNING), connectionActualStates.getAllValues());
    }
    
    @Test
    public void testConnectorToConnectionShouldUpdateConnectionActualStateWithFailedUpdatedConnectorState() {
        Mockito.when(connectionService.query(ArgumentMatchers.any())).thenReturn(fakeInitializedConnectionDtoList());
        Mockito.when(connectorService.query(ArgumentMatchers.any())).thenReturn(fakeFailedConnectorDtoList());
        
        connectionController.start();
        
        ArgumentCaptor<ConnectionState> connectionActualStates = ArgumentCaptor.forClass(ConnectionState.class);
        Mockito.verify(connectionService, Mockito.times(4)).updateActualState(ArgumentMatchers.any(), connectionActualStates.capture());
        Assert.assertEquals(Lists.newArrayList(ConnectionState.STOPPING, ConnectionState.STARTING, ConnectionState.FAILED, ConnectionState.FAILED), connectionActualStates.getAllValues());
    }
    
    private List<ConnectionDTO> fakeNewConnectionDtoList() {
        return Lists.newArrayList(
                new ConnectionDTO().setId(1L).setDeleted(false).setActualState(ConnectionState.STOPPED)
                        .setDesiredState(ConnectionState.RUNNING).setUpdateTime(Date.from(Instant.parse("2022-06-06T10:15:30Z"))),
                new ConnectionDTO().setId(2L).setDeleted(false).setActualState(ConnectionState.STOPPED)
                        .setDesiredState(ConnectionState.STOPPED).setUpdateTime(Date.from(Instant.parse("2022-06-06T10:25:30Z")))
        );
    }
    
    private List<ConnectionDTO> fakeReturnConnectionDtoList() {
        return Lists.newArrayList(
                new ConnectionDTO().setId(1L).setSourceConnectorId(1001L).setSinkConnectorId(2001L).setDeleted(false)
                        .setActualState(ConnectionState.STOPPED).setDesiredState(ConnectionState.RUNNING).setUpdateTime(Date.from(Instant.parse("2022-06-06T10:15:30Z"))),
                new ConnectionDTO().setId(2L).setSourceConnectorId(1002L).setSinkConnectorId(2002L).setDeleted(false)
                        .setActualState(ConnectionState.STOPPED).setDesiredState(ConnectionState.STOPPED).setUpdateTime(Date.from(Instant.parse("2022-06-06T10:25:30Z")))
        );
    }
    
    private List<ConnectionDTO> fakeInitializedUpdatedConnectionDtoList() {
        return Lists.newArrayList(
                new ConnectionDTO().setId(1L).setSourceConnectorId(1001L).setSinkConnectorId(2001L).setDeleted(false)
                        .setActualState(ConnectionState.RUNNING).setDesiredState(ConnectionState.STOPPED).setUpdateTime(Date.from(Instant.parse("2022-06-06T10:15:30Z"))),
                new ConnectionDTO().setId(2L).setSourceConnectorId(1002L).setSinkConnectorId(2002L).setDeleted(false)
                        .setActualState(ConnectionState.STOPPED).setDesiredState(ConnectionState.RUNNING).setUpdateTime(Date.from(Instant.parse("2022-06-06T10:25:30Z")))
        );
    }
    
    private List<ConnectionDTO> fakeInitializedConnectionDtoList() {
        return Lists.newArrayList(
                new ConnectionDTO().setId(1L).setSourceConnectorId(1001L).setSinkConnectorId(2001L).setDeleted(false)
                        .setActualState(ConnectionState.RUNNING).setDesiredState(ConnectionState.STOPPED).setUpdateTime(Date.from(Instant.parse("2022-06-06T10:15:30Z"))),
                new ConnectionDTO().setId(2L).setSourceConnectorId(1002L).setSinkConnectorId(2002L).setDeleted(false)
                        .setActualState(ConnectionState.STOPPED).setDesiredState(ConnectionState.RUNNING).setUpdateTime(Date.from(Instant.parse("2022-06-06T10:25:30Z")))
        );
    }
    
    private List<ConnectorDTO> fakeIncompleteConnectorDtoList() {
        return Lists.newArrayList(
                new ConnectorDTO().setConnectorType(ConnectorType.SINK).setId(2001L).setActualState(ConnectorState.STOPPED)
                        .setDesiredState(ConnectorState.STOPPED).setUpdateTime(Date.from(Instant.parse("2022-06-06T10:15:30Z"))),
                new ConnectorDTO().setConnectorType(ConnectorType.SOURCE).setId(1002L).setActualState(ConnectorState.RUNNING)
                        .setDesiredState(ConnectorState.RUNNING).setUpdateTime(Date.from(Instant.parse("2022-06-06T10:25:30Z")))
        );
    }
    
    private List<ConnectorDTO> fakeConnectorDtoList() {
        return Lists.newArrayList(
                new ConnectorDTO().setConnectorType(ConnectorType.SINK).setId(2001L).setActualState(ConnectorState.STOPPED)
                        .setDesiredState(ConnectorState.STOPPED).setUpdateTime(Date.from(Instant.parse("2022-06-06T10:15:30Z"))),
                new ConnectorDTO().setConnectorType(ConnectorType.SOURCE).setId(1002L).setActualState(ConnectorState.RUNNING)
                        .setDesiredState(ConnectorState.RUNNING).setUpdateTime(Date.from(Instant.parse("2022-06-06T10:25:30Z"))),
                new ConnectorDTO().setConnectorType(ConnectorType.SOURCE).setId(1001L).setActualState(ConnectorState.RUNNING)
                        .setDesiredState(ConnectorState.RUNNING).setUpdateTime(Date.from(Instant.parse("2022-06-06T10:15:30Z"))),
                new ConnectorDTO().setConnectorType(ConnectorType.SINK).setId(2002L).setActualState(ConnectorState.RUNNING)
                        .setDesiredState(ConnectorState.RUNNING).setUpdateTime(Date.from(Instant.parse("2022-06-06T10:25:30Z")))
        );
    }
    
    private List<ConnectorDTO> fakeFailedConnectorDtoList() {
        return Lists.newArrayList(
                new ConnectorDTO().setConnectorType(ConnectorType.SOURCE).setId(1001L).setActualState(ConnectorState.RUNNING)
                        .setDesiredState(ConnectorState.RUNNING).setUpdateTime(Date.from(Instant.parse("2022-06-06T10:15:30Z"))),
                new ConnectorDTO().setConnectorType(ConnectorType.SINK).setId(2001L).setActualState(ConnectorState.RUNTIME_FAILED)
                        .setDesiredState(ConnectorState.RUNNING).setUpdateTime(Date.from(Instant.parse("2022-06-06T10:15:30Z"))),
                new ConnectorDTO().setConnectorType(ConnectorType.SOURCE).setId(1002L).setActualState(ConnectorState.RUNNING)
                        .setDesiredState(ConnectorState.RUNNING).setUpdateTime(Date.from(Instant.parse("2022-06-06T10:25:30Z"))),
                new ConnectorDTO().setConnectorType(ConnectorType.SINK).setId(2002L).setActualState(ConnectorState.CREATION_FAILED)
                        .setDesiredState(ConnectorState.RUNNING).setUpdateTime(Date.from(Instant.parse("2022-06-06T10:25:30Z")))
        );
    }
}
