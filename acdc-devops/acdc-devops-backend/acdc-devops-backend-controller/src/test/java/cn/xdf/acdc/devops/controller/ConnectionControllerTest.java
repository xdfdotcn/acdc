package cn.xdf.acdc.devops.controller;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import cn.xdf.acdc.devops.core.domain.query.ConnectionQuery;
import cn.xdf.acdc.devops.service.process.connector.ConnectorCoreProcessService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorQueryProcessService;
import cn.xdf.acdc.devops.service.process.connection.ConnectionProcessService;
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
import java.util.List;
import java.util.concurrent.ScheduledFuture;

@RunWith(MockitoJUnitRunner.class)
public class ConnectionControllerTest {

    @Mock
    private ConnectionProcessService connectionProcessService;

    @Mock
    private ConnectorCoreProcessService connectorCoreProcessService;

    @Mock
    private ConnectorQueryProcessService connectorQueryProcessService;

    private ConnectionController connectionController;

    private final TaskScheduler taskScheduler = fakeTaskScheduler();

    private volatile int scheduleTimes = 1;

    @Before
    public void setup() {
        connectionController = new ConnectionController(taskScheduler, connectionProcessService, connectorCoreProcessService, connectorQueryProcessService);
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
        List<ConnectionDetailDTO> connectionDetailDtos = fakeNewConnectionDtoList();
        ConnectionDetailDTO runningConnection = connectionDetailDtos.get(0);
        Mockito.when(connectionProcessService.query(ArgumentMatchers.any())).thenReturn(connectionDetailDtos);
        Mockito.when(connectionProcessService.applyConnectionToConnector(ArgumentMatchers.any()))
                .thenReturn(fakeReturnConnectionDtoList().get(0))
                .thenReturn(fakeReturnConnectionDtoList().get(1));

        connectionController.start();
        ArgumentCaptor<ConnectionDetailDTO> connectionDtoCaptor = ArgumentCaptor.forClass(ConnectionDetailDTO.class);
        Mockito.verify(connectionProcessService, Mockito.times(1)).applyConnectionToConnector(connectionDtoCaptor.capture());
        Assert.assertEquals(runningConnection, connectionDtoCaptor.getValue());
    }

    @Test
    public void testConnectionToConnectorShouldStopConnectorWithDeletedConnection() {
        Mockito.when(connectionProcessService.query(ArgumentMatchers.any())).thenReturn(fakeDeleteConnectionDtoList());
        connectionController.start();
        ArgumentCaptor<Long> ids = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectorState> states = ArgumentCaptor.forClass(ConnectorState.class);
        Mockito.verify(connectorCoreProcessService, Mockito.times(2)).updateDesiredState(ids.capture(), states.capture());
        Assert.assertEquals(Lists.newArrayList(2001L, 2002L), ids.getAllValues());
        Assert.assertEquals(Lists.newArrayList(ConnectorState.STOPPED, ConnectorState.STOPPED), states.getAllValues());
    }

    @Test
    public void testQueryShouldUpdateBeginQueryTime() {
        Mockito.when(connectionProcessService.query(ArgumentMatchers.any())).thenReturn(fakeNewConnectionDtoList());
        Mockito.when(connectionProcessService.applyConnectionToConnector(ArgumentMatchers.any()))
                .thenReturn(fakeReturnConnectionDtoList().get(0))
                .thenReturn(fakeReturnConnectionDtoList().get(1));

        scheduleTimes = 2;
        connectionController.start();
        ArgumentCaptor<ConnectionQuery> connectionQueryCaptor = ArgumentCaptor.forClass(ConnectionQuery.class);
        Mockito.verify(connectionProcessService, Mockito.times(2)).query(connectionQueryCaptor.capture());
        Assert.assertEquals(
                Lists.newArrayList(
                        ConnectionQuery.builder().beginUpdateTime(Instant.ofEpochSecond(0)).requisitionState(RequisitionState.APPROVED).build(),
                        ConnectionQuery.builder().beginUpdateTime(Instant.parse("2022-06-06T10:25:30Z")).requisitionState(RequisitionState.APPROVED).build()
                ),
                connectionQueryCaptor.getAllValues());
    }

    @Test
    public void testConnectionToConnectorShouldUpdateConnectorWithInitializedUpdatedConnection() {
        List<ConnectionDetailDTO> fakeConnectionDetailDtoList = fakeInitializedConnectionDtoList();
        ConnectionDetailDTO expectUpdateConnectorStateAndConfig = fakeConnectionDetailDtoList.get(1);

        Mockito.when(connectionProcessService.query(ArgumentMatchers.any())).thenReturn(fakeConnectionDetailDtoList);

        connectionController.start();

        ArgumentCaptor<ConnectionDetailDTO> connectionDtoCaptor = ArgumentCaptor.forClass(ConnectionDetailDTO.class);
        Mockito.verify(connectionProcessService, Mockito.times(1)).flushConnectionConfigToConnector(connectionDtoCaptor.capture());
        Assert.assertEquals(expectUpdateConnectorStateAndConfig, connectionDtoCaptor.getValue());

        ArgumentCaptor<Long> connectorIds = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectorState> connectorDesiredStates = ArgumentCaptor.forClass(ConnectorState.class);
        Mockito.verify(connectorCoreProcessService, Mockito.times(2)).updateDesiredState(connectorIds.capture(), connectorDesiredStates.capture());
        Assert.assertEquals(Lists.newArrayList(2001L, 2002L), connectorIds.getAllValues());
        Assert.assertEquals(Lists.newArrayList(ConnectorState.STOPPED, ConnectorState.RUNNING), connectorDesiredStates.getAllValues());

        ArgumentCaptor<Long> connectionIds = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectionState> connectionActualStates = ArgumentCaptor.forClass(ConnectionState.class);
        Mockito.verify(connectionProcessService, Mockito.times(2)).editActualState(connectionIds.capture(), connectionActualStates.capture());
        Assert.assertEquals(Lists.newArrayList(1L, 2L), connectionIds.getAllValues());
        Assert.assertEquals(Lists.newArrayList(ConnectionState.STOPPING, ConnectionState.STARTING), connectionActualStates.getAllValues());
    }

    @Test
    public void testConnectionToConnectorShouldUpdateConnectorWithUpdatedConnection() {
        List<ConnectionDetailDTO> fakeConnectionDetailDtoList = fakeInitializedUpdatedConnectionDtoList();
        ConnectionDetailDTO expectUpdateConnectorStateAndConfig = fakeConnectionDetailDtoList.get(1);

        Mockito.when(connectionProcessService.query(ArgumentMatchers.any()))
                .thenReturn(fakeNewConnectionDtoList())
                .thenReturn(fakeConnectionDetailDtoList);

        Mockito.when(connectionProcessService.applyConnectionToConnector(ArgumentMatchers.any()))
                .thenReturn(fakeReturnConnectionDtoList().get(0))
                .thenReturn(fakeReturnConnectionDtoList().get(1));

        scheduleTimes = 2;
        connectionController.start();

        ArgumentCaptor<ConnectionDetailDTO> connectionDtoCaptor = ArgumentCaptor.forClass(ConnectionDetailDTO.class);
        Mockito.verify(connectionProcessService, Mockito.times(1)).flushConnectionConfigToConnector(connectionDtoCaptor.capture());
        Assert.assertEquals(expectUpdateConnectorStateAndConfig, connectionDtoCaptor.getValue());

        ArgumentCaptor<Long> connectorIds = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectorState> connectorDesiredStates = ArgumentCaptor.forClass(ConnectorState.class);
        Mockito.verify(connectorCoreProcessService, Mockito.times(2)).updateDesiredState(connectorIds.capture(), connectorDesiredStates.capture());
        Assert.assertEquals(Lists.newArrayList(2001L, 2002L), connectorIds.getAllValues());
        Assert.assertEquals(Lists.newArrayList(ConnectorState.STOPPED, ConnectorState.RUNNING), connectorDesiredStates.getAllValues());

        ArgumentCaptor<Long> connectionIds = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectionState> connectionActualStates = ArgumentCaptor.forClass(ConnectionState.class);
        Mockito.verify(connectionProcessService, Mockito.times(3)).editActualState(connectionIds.capture(), connectionActualStates.capture());
        Assert.assertEquals(Lists.newArrayList(1L, 1L, 2L), connectionIds.getAllValues());
        Assert.assertEquals(Lists.newArrayList(ConnectionState.STARTING, ConnectionState.STOPPING, ConnectionState.STARTING), connectionActualStates.getAllValues());
    }

    @Test
    public void testConnectorToConnectionShouldIgnoreWithIncompleteConnectors() {
        Mockito.when(connectionProcessService.query(ArgumentMatchers.any())).thenReturn(fakeInitializedConnectionDtoList());
        Mockito.when(connectorQueryProcessService.query(ArgumentMatchers.any())).thenReturn(fakeIncompleteConnectorDtoList());

        connectionController.start();

        ArgumentCaptor<ConnectionState> connectionActualStates = ArgumentCaptor.forClass(ConnectionState.class);
        Mockito.verify(connectionProcessService, Mockito.times(2)).editActualState(ArgumentMatchers.any(), connectionActualStates.capture());
        Assert.assertEquals(Lists.newArrayList(ConnectionState.STOPPING, ConnectionState.STARTING), connectionActualStates.getAllValues());
    }

    @Test
    public void testConnectorToConnectionShouldUpdateConnectionActualStateWithUpdatedConnectorState() {
        Mockito.when(connectionProcessService.query(ArgumentMatchers.any())).thenReturn(fakeInitializedConnectionDtoList());
        Mockito.when(connectorQueryProcessService.query(ArgumentMatchers.any())).thenReturn(fakeConnectorDtoList());

        connectionController.start();

        ArgumentCaptor<ConnectionState> connectionActualStates = ArgumentCaptor.forClass(ConnectionState.class);
        Mockito.verify(connectionProcessService, Mockito.times(4)).editActualState(ArgumentMatchers.any(), connectionActualStates.capture());
        Assert.assertEquals(Lists.newArrayList(ConnectionState.STOPPING, ConnectionState.STARTING, ConnectionState.STOPPED, ConnectionState.RUNNING), connectionActualStates.getAllValues());
    }

    @Test
    public void testConnectorToConnectionShouldUpdateConnectionActualStateWithFailedUpdatedConnectorState() {
        Mockito.when(connectionProcessService.query(ArgumentMatchers.any())).thenReturn(fakeInitializedConnectionDtoList());
        Mockito.when(connectorQueryProcessService.query(ArgumentMatchers.any())).thenReturn(fakeFailedConnectorDtoList());

        connectionController.start();

        ArgumentCaptor<ConnectionState> connectionActualStates = ArgumentCaptor.forClass(ConnectionState.class);
        Mockito.verify(connectionProcessService, Mockito.times(4)).editActualState(ArgumentMatchers.any(), connectionActualStates.capture());
        Assert.assertEquals(Lists.newArrayList(ConnectionState.STOPPING, ConnectionState.STARTING, ConnectionState.FAILED, ConnectionState.FAILED), connectionActualStates.getAllValues());
    }

    private List<ConnectionDetailDTO> fakeNewConnectionDtoList() {
        return Lists.newArrayList(
                ConnectionDetailDTO.builder().id(1L).deleted(false).actualState(ConnectionState.STOPPED)
                        .desiredState(ConnectionState.RUNNING).updateTime(Instant.parse("2022-06-06T10:15:30Z")).build(),
                ConnectionDetailDTO.builder().id(2L).deleted(false).actualState(ConnectionState.STOPPED)
                        .desiredState(ConnectionState.STOPPED).updateTime(Instant.parse("2022-06-06T10:25:30Z")).build()
        );
    }

    private List<ConnectionDetailDTO> fakeReturnConnectionDtoList() {
        return Lists.newArrayList(
                ConnectionDetailDTO.builder().id(1L).sourceConnectorId(1001L).sinkConnectorId(2001L).deleted(false)
                        .actualState(ConnectionState.STOPPED).desiredState(ConnectionState.RUNNING).updateTime(Instant.parse("2022-06-06T10:15:30Z")).build(),
                ConnectionDetailDTO.builder().id(2L).sourceConnectorId(1002L).sinkConnectorId(2002L).deleted(false)
                        .actualState(ConnectionState.STOPPED).desiredState(ConnectionState.STOPPED).updateTime(Instant.parse("2022-06-06T10:25:30Z")).build()
        );
    }

    private List<ConnectionDetailDTO> fakeDeleteConnectionDtoList() {
        return Lists.newArrayList(
                ConnectionDetailDTO.builder().id(1L).sourceConnectorId(1001L).sinkConnectorId(2001L).deleted(true)
                        .actualState(ConnectionState.STOPPED).desiredState(ConnectionState.STOPPED).updateTime(Instant.parse("2022-06-06T10:15:30Z")).build(),
                ConnectionDetailDTO.builder().id(2L).sourceConnectorId(1002L).sinkConnectorId(2002L).deleted(true)
                        .actualState(ConnectionState.STOPPED).desiredState(ConnectionState.STOPPED).updateTime(Instant.parse("2022-06-06T10:25:30Z")).build()
        );
    }

    private List<ConnectionDetailDTO> fakeInitializedUpdatedConnectionDtoList() {
        return Lists.newArrayList(
                ConnectionDetailDTO.builder().id(1L).sourceConnectorId(1001L).sinkConnectorId(2001L).deleted(false)
                        .actualState(ConnectionState.STOPPED).desiredState(ConnectionState.STOPPED).updateTime(Instant.parse("2022-06-06T10:15:30Z")).build(),
                ConnectionDetailDTO.builder().id(2L).sourceConnectorId(1002L).sinkConnectorId(2002L).deleted(false)
                        .actualState(ConnectionState.STOPPED).desiredState(ConnectionState.RUNNING).updateTime(Instant.parse("2022-06-06T10:25:30Z")).build()
        );
    }

    private List<ConnectionDetailDTO> fakeInitializedConnectionDtoList() {
        return Lists.newArrayList(
                ConnectionDetailDTO.builder().id(1L).sourceConnectorId(1001L).sinkConnectorId(2001L).deleted(false)
                        .actualState(ConnectionState.RUNNING).desiredState(ConnectionState.STOPPED).updateTime(Instant.parse("2022-06-06T10:15:30Z")).build(),
                ConnectionDetailDTO.builder().id(2L).sourceConnectorId(1002L).sinkConnectorId(2002L).deleted(false)
                        .actualState(ConnectionState.STOPPED).desiredState(ConnectionState.RUNNING).updateTime(Instant.parse("2022-06-06T10:25:30Z")).build()
        );
    }

    private List<ConnectorDTO> fakeIncompleteConnectorDtoList() {
        return Lists.newArrayList(
                ConnectorDTO.builder().connectorType(ConnectorType.SINK).id(2001L).actualState(ConnectorState.STOPPED)
                        .desiredState(ConnectorState.STOPPED).updateTime(Instant.parse("2022-06-06T10:15:30Z")).build(),
                ConnectorDTO.builder().connectorType(ConnectorType.SOURCE).id(1002L).actualState(ConnectorState.RUNNING)
                        .desiredState(ConnectorState.RUNNING).updateTime(Instant.parse("2022-06-06T10:25:30Z")).build()
        );
    }

    private List<ConnectorDTO> fakeConnectorDtoList() {
        return Lists.newArrayList(
                ConnectorDTO.builder().connectorType(ConnectorType.SINK).id(2001L).actualState(ConnectorState.STOPPED)
                        .desiredState(ConnectorState.STOPPED).updateTime(Instant.parse("2022-06-06T10:15:30Z")).build(),
                ConnectorDTO.builder().connectorType(ConnectorType.SOURCE).id(1002L).actualState(ConnectorState.RUNNING)
                        .desiredState(ConnectorState.RUNNING).updateTime(Instant.parse("2022-06-06T10:25:30Z")).build(),
                ConnectorDTO.builder().connectorType(ConnectorType.SOURCE).id(1001L).actualState(ConnectorState.RUNNING)
                        .desiredState(ConnectorState.RUNNING).updateTime(Instant.parse("2022-06-06T10:15:30Z")).build(),
                ConnectorDTO.builder().connectorType(ConnectorType.SINK).id(2002L).actualState(ConnectorState.RUNNING)
                        .desiredState(ConnectorState.RUNNING).updateTime(Instant.parse("2022-06-06T10:25:30Z")).build()
        );
    }

    private List<ConnectorDTO> fakeFailedConnectorDtoList() {
        return Lists.newArrayList(
                ConnectorDTO.builder().connectorType(ConnectorType.SOURCE).id(1001L).actualState(ConnectorState.RUNNING)
                        .desiredState(ConnectorState.RUNNING).updateTime(Instant.parse("2022-06-06T10:15:30Z")).build(),
                ConnectorDTO.builder().connectorType(ConnectorType.SINK).id(2001L).actualState(ConnectorState.RUNTIME_FAILED)
                        .desiredState(ConnectorState.RUNNING).updateTime(Instant.parse("2022-06-06T10:15:30Z")).build(),
                ConnectorDTO.builder().connectorType(ConnectorType.SOURCE).id(1002L).actualState(ConnectorState.RUNNING)
                        .desiredState(ConnectorState.RUNNING).updateTime(Instant.parse("2022-06-06T10:25:30Z")).build(),
                ConnectorDTO.builder().connectorType(ConnectorType.SINK).id(2002L).actualState(ConnectorState.CREATION_FAILED)
                        .desiredState(ConnectorState.RUNNING).updateTime(Instant.parse("2022-06-06T10:25:30Z")).build()
        );
    }
}
