package cn.xdf.acdc.devops.statemachine;

import cn.xdf.acdc.devops.biz.connect.ConnectClusterRest;
import cn.xdf.acdc.devops.biz.connect.response.ConnectorStatusResponse;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectClusterDO;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import cn.xdf.acdc.devops.service.entity.ConnectClusterService;
import cn.xdf.acdc.devops.service.entity.ConnectorEventService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorCoreProcessService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.assertj.core.util.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.squirrelframework.foundation.fsm.StateMachineStatus;

@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(classes = {StatemachineTestConfig.class})
public class ConnectorStateHandlerTest {

    private static final long CONNECT_CLUSTER_ID = 1L;

    private static final String CONNECT_CLUSTER_URL = "http://test:8083";

    @MockBean
    private ConnectClusterRest connectClusterRest;

    @MockBean
    private ConnectorEventService connectorEventService;

    @MockBean
    private ConnectorCoreProcessService connectorCoreProcessService;

    @MockBean
    private ConnectClusterService connectClusterService;

    @MockBean
    private MeterRegistry meterRegistry;

    @Autowired
    private ConnectorStateHandler connectorStateHandler;

    @Before
    public void setup() {
        connectorStateHandler.getStateMachineHolder().clear();
    }

    // U:Startup , clusterId:1
    @Test
    public void testPendingToStarting() {
        Assert.assertEquals(6, connectorStateHandler.getUserTriggerEventHandlers().size());
        List<ConnectorInfoDTO> connectorInfos = Lists.newArrayList(new ConnectorInfoDTO(1L, CONNECT_CLUSTER_URL, "pendingToStarting", new HashMap<>(), ConnectorState.PENDING, ConnectorState.RUNNING));
        Mockito.when(connectorCoreProcessService.queryConnector(ArgumentMatchers.eq(ConnectorState.PENDING), ArgumentMatchers.eq(ConnectorState.RUNNING), ArgumentMatchers.eq(CONNECT_CLUSTER_ID)))
                .thenReturn(connectorInfos);

        connectorStateHandler.getUserTriggerEventHandlers().forEach((event, handler) -> {
            handler.accept(CONNECT_CLUSTER_ID, event);
        });
        Map<Long, ConnectorStateMachine> stateMachineHolder = connectorStateHandler.getStateMachineHolder();
        Assert.assertEquals(ConnectorState.STARTING, stateMachineHolder.get(1L).getCurrentState());

        ArgumentCaptor<String> connectClusterUrlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> connectorNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map> connectorConfigCaptor = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(connectClusterRest).createConnector(connectClusterUrlCaptor.capture(), connectorNameCaptor.capture(), connectorConfigCaptor.capture());
        Assert.assertEquals(CONNECT_CLUSTER_URL, connectClusterUrlCaptor.getValue());
        Assert.assertEquals("pendingToStarting", connectorNameCaptor.getValue());
        Assert.assertEquals(new HashMap(), connectorConfigCaptor.getValue());

        ArgumentCaptor<Long> connectorIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectorState> stateCaptor = ArgumentCaptor.forClass(ConnectorState.class);
        Mockito.verify(connectorCoreProcessService).updateActualState(connectorIdCaptor.capture(), stateCaptor.capture());
        Assert.assertEquals((Long) 1L, connectorIdCaptor.getValue());
        Assert.assertEquals(ConnectorState.STARTING, stateCaptor.getValue());
    }

    // U:stop, clusterId:1
    @Test
    public void testPendingToStopping() {
        List<ConnectorInfoDTO> connectorInfos = Lists.newArrayList(new ConnectorInfoDTO(1L, CONNECT_CLUSTER_URL, "pendingToStopping", new HashMap<>(), ConnectorState.PENDING, ConnectorState.STOPPED));
        Mockito.when(connectorCoreProcessService.queryConnector(ArgumentMatchers.eq(ConnectorState.PENDING), ArgumentMatchers.eq(ConnectorState.STOPPED), ArgumentMatchers.eq(CONNECT_CLUSTER_ID)))
                .thenReturn(connectorInfos);

        connectorStateHandler.getUserTriggerEventHandlers().forEach((event, handler) -> {
            handler.accept(CONNECT_CLUSTER_ID, event);
        });
        Map<Long, ConnectorStateMachine> stateMachineHolder = connectorStateHandler.getStateMachineHolder();

        Assert.assertEquals(ConnectorState.STOPPING, stateMachineHolder.get(1L).getCurrentState());

        ArgumentCaptor<String> connectClusterUrlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> connectorNameCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(connectClusterRest).deleteConnector(connectClusterUrlCaptor.capture(), connectorNameCaptor.capture());
        Assert.assertEquals(CONNECT_CLUSTER_URL, connectClusterUrlCaptor.getValue());
        Assert.assertEquals("pendingToStopping", connectorNameCaptor.getValue());

        ArgumentCaptor<Long> connectorIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectorState> stateCaptor = ArgumentCaptor.forClass(ConnectorState.class);
        Mockito.verify(connectorCoreProcessService).updateActualState(connectorIdCaptor.capture(), stateCaptor.capture());
        Assert.assertEquals((Long) 1L, connectorIdCaptor.getValue());
        Assert.assertEquals(ConnectorState.STOPPING, stateCaptor.getValue());
    }

    // U:restart, clusterId:1
    @Test
    public void testStoppedToPending() {
        List<ConnectorInfoDTO> connectorInfos = Lists.newArrayList(new ConnectorInfoDTO(1L, CONNECT_CLUSTER_URL, "stoppedToPending", new HashMap<>(), ConnectorState.STOPPED, ConnectorState.RUNNING));
        Mockito.when(connectorCoreProcessService.queryConnector(ArgumentMatchers.eq(ConnectorState.STOPPED), ArgumentMatchers.eq(ConnectorState.RUNNING), ArgumentMatchers.eq(CONNECT_CLUSTER_ID)))
                .thenReturn(connectorInfos);

        connectorStateHandler.getUserTriggerEventHandlers().forEach((event, handler) -> {
            handler.accept(CONNECT_CLUSTER_ID, event);
        });
        Map<Long, ConnectorStateMachine> stateMachineHolder = connectorStateHandler.getStateMachineHolder();

        Assert.assertEquals(ConnectorState.PENDING, stateMachineHolder.get(1L).getCurrentState());

        ArgumentCaptor<Long> connectorIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectorState> stateCaptor = ArgumentCaptor.forClass(ConnectorState.class);
        Mockito.verify(connectorCoreProcessService).updateActualState(connectorIdCaptor.capture(), stateCaptor.capture());
        Assert.assertEquals((Long) 1L, connectorIdCaptor.getValue());
        Assert.assertEquals(ConnectorState.PENDING, stateCaptor.getValue());
    }

    // U:stop, clusterId:1
    @Test
    public void testCreationFailedToStopped() {
        List<ConnectorInfoDTO> connectorInfos = Lists
                .newArrayList(new ConnectorInfoDTO(1L, CONNECT_CLUSTER_URL, "creationFailedToStopped", new HashMap<>(), ConnectorState.CREATION_FAILED, ConnectorState.STOPPED));
        Mockito.when(
                connectorCoreProcessService.queryConnector(ArgumentMatchers.eq(ConnectorState.CREATION_FAILED), ArgumentMatchers.eq(ConnectorState.STOPPED), ArgumentMatchers.eq(CONNECT_CLUSTER_ID)))
                .thenReturn(connectorInfos);

        connectorStateHandler.getUserTriggerEventHandlers().forEach((event, handler) -> {
            handler.accept(CONNECT_CLUSTER_ID, event);
        });
        Map<Long, ConnectorStateMachine> stateMachineHolder = connectorStateHandler.getStateMachineHolder();

        Assert.assertEquals(ConnectorState.STOPPED, stateMachineHolder.get(1L).getCurrentState());

        ArgumentCaptor<Long> connectorIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectorState> stateCaptor = ArgumentCaptor.forClass(ConnectorState.class);
        Mockito.verify(connectorCoreProcessService).updateActualState(connectorIdCaptor.capture(), stateCaptor.capture());
        Assert.assertEquals((Long) 1L, connectorIdCaptor.getValue());
        Assert.assertEquals(ConnectorState.STOPPED, stateCaptor.getValue());
    }

    // U:stop, clusterId:1
    @Test
    public void testRuntimeFailedToStopping() {
        List<ConnectorInfoDTO> connectorInfos = Lists
                .newArrayList(new ConnectorInfoDTO(1L, CONNECT_CLUSTER_URL, "runtimeFailedToStopping", new HashMap<>(), ConnectorState.RUNTIME_FAILED, ConnectorState.STOPPED));
        Mockito.when(
                connectorCoreProcessService.queryConnector(ArgumentMatchers.eq(ConnectorState.RUNTIME_FAILED), ArgumentMatchers.eq(ConnectorState.STOPPED), ArgumentMatchers.eq(CONNECT_CLUSTER_ID)))
                .thenReturn(connectorInfos);

        connectorStateHandler.getUserTriggerEventHandlers().forEach((event, handler) -> {
            handler.accept(CONNECT_CLUSTER_ID, event);
        });
        Map<Long, ConnectorStateMachine> stateMachineHolder = connectorStateHandler.getStateMachineHolder();

        Assert.assertEquals(ConnectorState.STOPPING, stateMachineHolder.get(1L).getCurrentState());

        ArgumentCaptor<String> connectClusterUrlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> connectorNameCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(connectClusterRest).deleteConnector(connectClusterUrlCaptor.capture(), connectorNameCaptor.capture());
        Assert.assertEquals(CONNECT_CLUSTER_URL, connectClusterUrlCaptor.getValue());
        Assert.assertEquals("runtimeFailedToStopping", connectorNameCaptor.getValue());

        ArgumentCaptor<Long> connectorIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectorState> stateCaptor = ArgumentCaptor.forClass(ConnectorState.class);
        Mockito.verify(connectorCoreProcessService).updateActualState(connectorIdCaptor.capture(), stateCaptor.capture());
        Assert.assertEquals((Long) 1L, connectorIdCaptor.getValue());
        Assert.assertEquals(ConnectorState.STOPPING, stateCaptor.getValue());
    }

    // U:stop, clusterId:1
    @Test
    public void testRunningToStopping() {
        List<ConnectorInfoDTO> connectorInfos = Lists.newArrayList(new ConnectorInfoDTO(1L, CONNECT_CLUSTER_URL, "runningToStopping", new HashMap<>(), ConnectorState.RUNNING, ConnectorState.STOPPED));
        Mockito.when(connectorCoreProcessService.queryConnector(ArgumentMatchers.eq(ConnectorState.RUNNING), ArgumentMatchers.eq(ConnectorState.STOPPED), ArgumentMatchers.eq(CONNECT_CLUSTER_ID)))
                .thenReturn(connectorInfos);

        connectorStateHandler.getUserTriggerEventHandlers().forEach((event, handler) -> {
            handler.accept(CONNECT_CLUSTER_ID, event);
        });

        Map<Long, ConnectorStateMachine> stateMachineHolder = connectorStateHandler.getStateMachineHolder();
        Assert.assertEquals(ConnectorState.STOPPING, stateMachineHolder.get(1L).getCurrentState());

        ArgumentCaptor<String> connectClusterUrlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> connectorNameCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(connectClusterRest).deleteConnector(connectClusterUrlCaptor.capture(), connectorNameCaptor.capture());
        Assert.assertEquals(CONNECT_CLUSTER_URL, connectClusterUrlCaptor.getValue());
        Assert.assertEquals("runningToStopping", connectorNameCaptor.getValue());

        ArgumentCaptor<Long> connectorIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectorState> stateCaptor = ArgumentCaptor.forClass(ConnectorState.class);
        Mockito.verify(connectorCoreProcessService).updateActualState(connectorIdCaptor.capture(), stateCaptor.capture());
        Assert.assertEquals((Long) 1L, connectorIdCaptor.getValue());
        Assert.assertEquals(ConnectorState.STOPPING, stateCaptor.getValue());
    }

    // W:StartupSuccess, clusterId:1
    @Test
    public void testStartingToRunning() throws JsonProcessingException {
        String connectorName = "startingToRunning";
        Mockito.when(connectClusterService.findById(ArgumentMatchers.any())).thenReturn(getConnectCluster());
        Mockito.when(connectClusterRest.getAllConnectorByClusterUrl(ArgumentMatchers.any())).thenReturn(Lists.newArrayList(connectorName));

        ConnectorStatusResponse statusResponse = getRunningResponseState(connectorName);
        Mockito.when(connectClusterRest.getConnectorStatus(ArgumentMatchers.eq(CONNECT_CLUSTER_URL), ArgumentMatchers.eq(connectorName))).thenReturn(statusResponse);

        Mockito.when(connectClusterRest.getConnectorConfig(ArgumentMatchers.eq(CONNECT_CLUSTER_URL), ArgumentMatchers.eq(connectorName))).thenReturn(Maps.newHashMap("config", "v1"));

        List<ConnectorInfoDTO> connectorInfos = Lists.newArrayList(new ConnectorInfoDTO(1L, CONNECT_CLUSTER_URL, connectorName, new HashMap<>(), ConnectorState.STARTING, ConnectorState.RUNNING));

        Mockito.when(connectorCoreProcessService.queryConnector(ArgumentMatchers.anyLong(), ArgumentMatchers.eq(ConnectorState.STARTING))).thenReturn(connectorInfos);

        connectorStateHandler.connectClusterStateWatcher(CONNECT_CLUSTER_ID);

        Map<Long, ConnectorStateMachine> stateMachineHolder = connectorStateHandler.getStateMachineHolder();
        Assert.assertEquals(ConnectorState.RUNNING, stateMachineHolder.get(1L).getCurrentState());

        ArgumentCaptor<Long> connectorIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectorState> stateCaptor = ArgumentCaptor.forClass(ConnectorState.class);
        Mockito.verify(connectorCoreProcessService).updateActualState(connectorIdCaptor.capture(), stateCaptor.capture());
        Assert.assertEquals((Long) 1L, connectorIdCaptor.getValue());
        Assert.assertEquals(ConnectorState.RUNNING, stateCaptor.getValue());
    }

    private Optional<ConnectClusterDO> getConnectCluster() {
        ConnectClusterDO connectCluster = new ConnectClusterDO();
        connectCluster.setId(CONNECT_CLUSTER_ID);
        connectCluster.setConnectRestApiUrl(CONNECT_CLUSTER_URL);
        return Optional.of(connectCluster);
    }

    private ConnectorStatusResponse getRunningResponseState(final String connectorName) {
        ConnectorStatusResponse statusResponse = new ConnectorStatusResponse();
        statusResponse.setName(connectorName);
        statusResponse.setType("source");
        Map<String, String> connector = new HashMap<>();
        connector.put("id", "1");
        connector.put("state", "running");
        statusResponse.setConnector(connector);
        Map<String, String> tasks = new HashMap<>();
        tasks.put("1", "0");
        tasks.put("state", "running");
        statusResponse.setTasks(Lists.newArrayList(tasks));
        return statusResponse;
    }

    // W:TaskFailure, clusterId:1
    @Test
    public void testStartingToRuntimeFailed() throws JsonProcessingException {
        String connectorName = "startingToRuntimeFailed";
        Mockito.when(connectClusterService.findById(ArgumentMatchers.any())).thenReturn(getConnectCluster());
        Mockito.when(connectClusterRest.getAllConnectorByClusterUrl(ArgumentMatchers.any())).thenReturn(Lists.newArrayList(connectorName));

        ConnectorStatusResponse statusResponse = getFailedResponseState(connectorName);
        Mockito.when(connectClusterRest.getConnectorStatus(ArgumentMatchers.eq(CONNECT_CLUSTER_URL), ArgumentMatchers.eq(connectorName))).thenReturn(statusResponse);

        Mockito.when(connectClusterRest.getConnectorConfig(ArgumentMatchers.eq(CONNECT_CLUSTER_URL), ArgumentMatchers.eq(connectorName))).thenReturn(Maps.newHashMap("config", "v1"));

        List<ConnectorInfoDTO> connectorInfos = Lists.newArrayList(new ConnectorInfoDTO(1L, CONNECT_CLUSTER_URL, connectorName, new HashMap<>(), ConnectorState.STARTING, ConnectorState.RUNNING));

        Mockito.when(connectorCoreProcessService.queryConnector(ArgumentMatchers.anyLong(), ArgumentMatchers.eq(ConnectorState.STARTING))).thenReturn(connectorInfos);

        connectorStateHandler.connectClusterStateWatcher(CONNECT_CLUSTER_ID);

        Map<Long, ConnectorStateMachine> stateMachineHolder = connectorStateHandler.getStateMachineHolder();
        Assert.assertEquals(ConnectorState.RUNTIME_FAILED, stateMachineHolder.get(1L).getCurrentState());

        ArgumentCaptor<Long> connectorIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectorState> stateCaptor = ArgumentCaptor.forClass(ConnectorState.class);
        Mockito.verify(connectorCoreProcessService).updateActualState(connectorIdCaptor.capture(), stateCaptor.capture());
        Assert.assertEquals((Long) 1L, connectorIdCaptor.getValue());
        Assert.assertEquals(ConnectorState.RUNTIME_FAILED, stateCaptor.getValue());
    }

    private ConnectorStatusResponse getFailedResponseState(final String connectorName) {
        ConnectorStatusResponse statusResponse = new ConnectorStatusResponse();
        statusResponse.setName(connectorName);
        statusResponse.setType("source");
        Map<String, String> connector = new HashMap<>();
        connector.put("id", "1");
        connector.put("state", "failed");
        statusResponse.setConnector(connector);
        Map<String, String> tasks = new HashMap<>();
        tasks.put("1", "0");
        tasks.put("state", "failed");
        statusResponse.setTasks(Lists.newArrayList(tasks));
        return statusResponse;
    }

    // W:TaskFailure, clusterId:1
    @Test
    public void testRunningToRuntimeFailed() throws JsonProcessingException {
        String connectorName = "runningToRuntimeFailed";
        Mockito.when(connectClusterService.findById(ArgumentMatchers.any())).thenReturn(getConnectCluster());
        Mockito.when(connectClusterRest.getAllConnectorByClusterUrl(ArgumentMatchers.any())).thenReturn(Lists.newArrayList(connectorName));

        ConnectorStatusResponse statusResponse = getFailedResponseState(connectorName);
        Mockito.when(connectClusterRest.getConnectorStatus(ArgumentMatchers.eq(CONNECT_CLUSTER_URL), ArgumentMatchers.eq(connectorName))).thenReturn(statusResponse);

        Mockito.when(connectClusterRest.getConnectorConfig(ArgumentMatchers.eq(CONNECT_CLUSTER_URL), ArgumentMatchers.eq(connectorName))).thenReturn(Maps.newHashMap("config", "v1"));

        List<ConnectorInfoDTO> connectorInfos = Lists.newArrayList(new ConnectorInfoDTO(1L, CONNECT_CLUSTER_URL, connectorName, new HashMap<>(), ConnectorState.RUNNING, ConnectorState.RUNNING));

        Mockito.when(connectorCoreProcessService.queryConnector(ArgumentMatchers.eq(ConnectorState.RUNNING), ArgumentMatchers.eq(ConnectorState.RUNNING), ArgumentMatchers.anyLong()))
                .thenReturn(connectorInfos);

        connectorStateHandler.connectClusterStateWatcher(CONNECT_CLUSTER_ID);

        Map<Long, ConnectorStateMachine> stateMachineHolder = connectorStateHandler.getStateMachineHolder();
        Assert.assertEquals(ConnectorState.RUNTIME_FAILED, stateMachineHolder.get(1L).getCurrentState());

        ArgumentCaptor<Long> connectorIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectorState> stateCaptor = ArgumentCaptor.forClass(ConnectorState.class);
        Mockito.verify(connectorCoreProcessService).updateActualState(connectorIdCaptor.capture(), stateCaptor.capture());
        Assert.assertEquals((Long) 1L, connectorIdCaptor.getValue());
        Assert.assertEquals(ConnectorState.RUNTIME_FAILED, stateCaptor.getValue());
    }

    // W: update, clusterId:1
    @Test
    public void testRunningToUpdating() throws JsonProcessingException {
        String connectorName = "runningToUpdating";
        Mockito.when(connectClusterService.findById(ArgumentMatchers.any())).thenReturn(getConnectCluster());
        Mockito.when(connectClusterRest.getAllConnectorByClusterUrl(ArgumentMatchers.any())).thenReturn(Lists.newArrayList(connectorName));

        ConnectorStatusResponse statusResponse = getRunningResponseState(connectorName);
        Mockito.when(connectClusterRest.getConnectorStatus(ArgumentMatchers.eq(CONNECT_CLUSTER_URL), ArgumentMatchers.eq(connectorName))).thenReturn(statusResponse);

        Mockito.when(connectClusterRest.getConnectorConfig(ArgumentMatchers.eq(CONNECT_CLUSTER_URL), ArgumentMatchers.eq(connectorName))).thenReturn(Maps.newHashMap("config", "v1"));

        List<ConnectorInfoDTO> connectorInfos = Lists
                .newArrayList(new ConnectorInfoDTO(1L, CONNECT_CLUSTER_URL, connectorName, Maps.newHashMap("config", "v2"), ConnectorState.RUNNING, ConnectorState.RUNNING));

        Mockito.when(connectorCoreProcessService.queryConnector(ArgumentMatchers.eq(ConnectorState.RUNNING), ArgumentMatchers.eq(ConnectorState.RUNNING), ArgumentMatchers.anyLong()))
                .thenReturn(connectorInfos);

        connectorStateHandler.connectClusterStateWatcher(CONNECT_CLUSTER_ID);

        Map<Long, ConnectorStateMachine> stateMachineHolder = connectorStateHandler.getStateMachineHolder();
        Assert.assertEquals(ConnectorState.UPDATING, stateMachineHolder.get(1L).getCurrentState());

        ArgumentCaptor<String> connectClusterUrlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> connectorNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map> connectorConfigCaptor = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(connectClusterRest).putConnectorConfig(connectClusterUrlCaptor.capture(), connectorNameCaptor.capture(), connectorConfigCaptor.capture());
        Assert.assertEquals(CONNECT_CLUSTER_URL, connectClusterUrlCaptor.getValue());
        Assert.assertEquals(connectorName, connectorNameCaptor.getValue());
        // todo connectorConfigCaptor.capture() is null ?
        //  Assert.assertEquals(Maps.newHashMap("config", "v2"), connectorConfigCaptor.capture());

        ArgumentCaptor<Long> connectorIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectorState> stateCaptor = ArgumentCaptor.forClass(ConnectorState.class);
        Mockito.verify(connectorCoreProcessService).updateActualState(connectorIdCaptor.capture(), stateCaptor.capture());
        Assert.assertEquals((Long) 1L, connectorIdCaptor.getValue());
        Assert.assertEquals(ConnectorState.UPDATING, stateCaptor.getValue());
    }

    // W: StopSuccess, clusterId:1
    @Test
    public void testStoppingToStopped() {
        String connectorName = "stoppingToStopped";
        Mockito.when(connectClusterService.findById(ArgumentMatchers.any())).thenReturn(getConnectCluster());
        Mockito.when(connectClusterRest.getAllConnectorByClusterUrl(ArgumentMatchers.any())).thenReturn(Lists.newArrayList());

        List<ConnectorInfoDTO> connectorInfos = Lists.newArrayList(new ConnectorInfoDTO(1L, CONNECT_CLUSTER_URL, connectorName, new HashMap<>(), ConnectorState.STOPPING, ConnectorState.STOPPED));
        Mockito.when(connectorCoreProcessService.queryConnector(ArgumentMatchers.anyLong(), ArgumentMatchers.eq(ConnectorState.STOPPING))).thenReturn(connectorInfos);

        connectorStateHandler.connectClusterStateWatcher(CONNECT_CLUSTER_ID);

        Map<Long, ConnectorStateMachine> stateMachineHolder = connectorStateHandler.getStateMachineHolder();
        Assert.assertEquals(ConnectorState.STOPPED, stateMachineHolder.get(1L).getCurrentState());

        ArgumentCaptor<Long> connectorIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectorState> stateCaptor = ArgumentCaptor.forClass(ConnectorState.class);
        Mockito.verify(connectorCoreProcessService).updateActualState(connectorIdCaptor.capture(), stateCaptor.capture());
        Assert.assertEquals((Long) 1L, connectorIdCaptor.getValue());
        Assert.assertEquals(ConnectorState.STOPPED, stateCaptor.getValue());
    }

    // T: timeout
    @Test
    public void testStartingToPending() throws InterruptedException {
        connectorStateHandler.setConnectorExecTimeoutInMillisecond(1_000L);
        String connectorName = "startingToPending";
        Mockito.when(connectClusterService.findById(ArgumentMatchers.any())).thenReturn(getConnectCluster());

        Mockito.when(connectClusterRest.getAllConnectorByClusterUrl(ArgumentMatchers.eq(CONNECT_CLUSTER_URL))).thenReturn(Lists.newArrayList());

        List<ConnectorInfoDTO> connectorInfos = Lists.newArrayList(new ConnectorInfoDTO(1L, CONNECT_CLUSTER_URL, connectorName, new HashMap<>(), ConnectorState.STARTING, ConnectorState.RUNNING));

        Mockito.when(connectorCoreProcessService.queryConnector(ArgumentMatchers.eq(CONNECT_CLUSTER_ID), ArgumentMatchers.eq(ConnectorState.STARTING))).thenReturn(connectorInfos);

        connectorStateHandler.innerExtendEventWatcher(CONNECT_CLUSTER_ID);

        Map<Long, ConnectorStateMachine> stateMachineHolder = connectorStateHandler.getStateMachineHolder();
        Assert.assertEquals(0, stateMachineHolder.size());

        Thread.sleep(1_500L);
        connectorStateHandler.innerExtendEventWatcher(CONNECT_CLUSTER_ID);
        Assert.assertEquals(ConnectorState.PENDING, stateMachineHolder.get(1L).getCurrentState());

        ArgumentCaptor<Long> connectorIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectorState> stateCaptor = ArgumentCaptor.forClass(ConnectorState.class);
        Mockito.verify(connectorCoreProcessService).updateActualState(connectorIdCaptor.capture(), stateCaptor.capture());
        Assert.assertEquals((Long) 1L, connectorIdCaptor.getValue());
        Assert.assertEquals(ConnectorState.PENDING, stateCaptor.getValue());
    }

    // T: timeout
    @Test
    public void testStoppingToRunning() throws InterruptedException {
        connectorStateHandler.setConnectorExecTimeoutInMillisecond(1_000L);
        String connectorName = "stoppingToRunning";
        Mockito.when(connectClusterService.findById(ArgumentMatchers.any())).thenReturn(getConnectCluster());

        Mockito.when(connectClusterRest.getAllConnectorByClusterUrl(ArgumentMatchers.eq(CONNECT_CLUSTER_URL))).thenReturn(Lists.newArrayList(connectorName));

        List<ConnectorInfoDTO> connectorInfos = Lists.newArrayList(new ConnectorInfoDTO(1L, CONNECT_CLUSTER_URL, connectorName, new HashMap<>(), ConnectorState.STOPPING, ConnectorState.STOPPED));

        Mockito.when(connectorCoreProcessService.queryConnector(ArgumentMatchers.eq(CONNECT_CLUSTER_ID), ArgumentMatchers.eq(ConnectorState.STOPPING))).thenReturn(connectorInfos);

        connectorStateHandler.innerExtendEventWatcher(CONNECT_CLUSTER_ID);

        Map<Long, ConnectorStateMachine> stateMachineHolder = connectorStateHandler.getStateMachineHolder();
        Assert.assertEquals(0, stateMachineHolder.size());

        Thread.sleep(1_500L);
        connectorStateHandler.innerExtendEventWatcher(CONNECT_CLUSTER_ID);
        Assert.assertEquals(ConnectorState.RUNNING, stateMachineHolder.get(1L).getCurrentState());

        ArgumentCaptor<Long> connectorIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectorState> stateCaptor = ArgumentCaptor.forClass(ConnectorState.class);
        Mockito.verify(connectorCoreProcessService).updateActualState(connectorIdCaptor.capture(), stateCaptor.capture());
        Assert.assertEquals((Long) 1L, connectorIdCaptor.getValue());
        Assert.assertEquals(ConnectorState.RUNNING, stateCaptor.getValue());
    }

    // T+W: UpdateSuccess
    @Test
    public void testUpdatingToRunning() {
        String connectorName = "updatingToRunning";
        Mockito.when(connectClusterService.findById(ArgumentMatchers.any())).thenReturn(getConnectCluster());

        Mockito.when(connectClusterRest.getAllConnectorByClusterUrl(ArgumentMatchers.eq(CONNECT_CLUSTER_URL))).thenReturn(Lists.newArrayList(connectorName));

        List<ConnectorInfoDTO> connectorInfos = Lists.newArrayList(new ConnectorInfoDTO(1L, CONNECT_CLUSTER_URL, connectorName, new HashMap<>(), ConnectorState.UPDATING, ConnectorState.RUNNING));

        Mockito.when(connectorCoreProcessService.queryConnector(ArgumentMatchers.eq(CONNECT_CLUSTER_ID), ArgumentMatchers.eq(ConnectorState.UPDATING))).thenReturn(connectorInfos);

        connectorStateHandler.innerExtendEventWatcher(CONNECT_CLUSTER_ID);

        Map<Long, ConnectorStateMachine> stateMachineHolder = connectorStateHandler.getStateMachineHolder();
        Assert.assertEquals(ConnectorState.RUNNING, stateMachineHolder.get(1L).getCurrentState());

        ArgumentCaptor<Long> connectorIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectorState> stateCaptor = ArgumentCaptor.forClass(ConnectorState.class);
        Mockito.verify(connectorCoreProcessService).updateActualState(connectorIdCaptor.capture(), stateCaptor.capture());
        Assert.assertEquals((Long) 1L, connectorIdCaptor.getValue());
        Assert.assertEquals(ConnectorState.RUNNING, stateCaptor.getValue());
    }

    // T+W: creation failed
    @Test
    public void testPendingToCreationFailed() {
        String connectorName = "PendingToCreationFailed";
        Mockito.when(connectClusterService.findById(ArgumentMatchers.any())).thenReturn(getConnectCluster());

        Mockito.when(connectClusterRest.getAllConnectorByClusterUrl(ArgumentMatchers.eq(CONNECT_CLUSTER_URL))).thenReturn(Lists.newArrayList());

        List<ConnectorInfoDTO> connectorInfos = Lists.newArrayList(new ConnectorInfoDTO(1L, CONNECT_CLUSTER_URL, connectorName, new HashMap<>(), ConnectorState.PENDING, ConnectorState.RUNNING));

        Mockito.when(connectorCoreProcessService.queryConnector(ArgumentMatchers.eq(CONNECT_CLUSTER_ID), ArgumentMatchers.eq(ConnectorState.PENDING))).thenReturn(connectorInfos);

        Mockito.doThrow(new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR))
                .when(connectClusterRest).validConnectorConfig(ArgumentMatchers.eq(CONNECT_CLUSTER_URL), ArgumentMatchers.eq(connectorName), ArgumentMatchers.anyMap());

        connectorStateHandler.innerExtendEventWatcher(CONNECT_CLUSTER_ID);

        Map<Long, ConnectorStateMachine> stateMachineHolder = connectorStateHandler.getStateMachineHolder();
        Assert.assertEquals(ConnectorState.CREATION_FAILED, stateMachineHolder.get(1L).getCurrentState());

        ArgumentCaptor<Long> connectorIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectorState> stateCaptor = ArgumentCaptor.forClass(ConnectorState.class);
        Mockito.verify(connectorCoreProcessService).updateActualState(connectorIdCaptor.capture(), stateCaptor.capture());
        Assert.assertEquals((Long) 1L, connectorIdCaptor.getValue());
        Assert.assertEquals(ConnectorState.CREATION_FAILED, stateCaptor.getValue());
    }

    // E: retry
    @Test
    public void testCreationFailedToPending() throws InterruptedException {
        String connectorName = "CreationFailedToPending";
        Mockito.when(connectClusterService.findById(ArgumentMatchers.any())).thenReturn(getConnectCluster());

        Mockito.when(connectClusterRest.getAllConnectorByClusterUrl(ArgumentMatchers.eq(CONNECT_CLUSTER_URL))).thenReturn(Lists.newArrayList());

        List<ConnectorInfoDTO> connectorInfos = Lists
                .newArrayList(new ConnectorInfoDTO(8L, CONNECT_CLUSTER_URL, connectorName, new HashMap<>(), ConnectorState.CREATION_FAILED, ConnectorState.RUNNING));

        Mockito.when(connectorCoreProcessService.queryConnector(
                ArgumentMatchers.eq(ConnectorState.CREATION_FAILED), ArgumentMatchers.eq(ConnectorState.RUNNING), ArgumentMatchers.eq(CONNECT_CLUSTER_ID)))
                .thenReturn(connectorInfos);

        connectorStateHandler.innerExtendEventWatcher(CONNECT_CLUSTER_ID);

        Map<Long, ConnectorStateMachine> stateMachineHolder = connectorStateHandler.getStateMachineHolder();
        Assert.assertEquals(ConnectorState.PENDING, stateMachineHolder.get(8L).getCurrentState());

        ArgumentCaptor<Long> connectorIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectorState> stateCaptor = ArgumentCaptor.forClass(ConnectorState.class);
        Mockito.verify(connectorCoreProcessService).updateActualState(connectorIdCaptor.capture(), stateCaptor.capture());
        Assert.assertEquals((Long) 8L, connectorIdCaptor.getValue());
        Assert.assertEquals(ConnectorState.PENDING, stateCaptor.getValue());
    }

    // E: retry
    @Test
    public void testRuntimeFailedToStarting() {
        String connectorName = "runtimeFailedToStarting";
        Mockito.when(connectClusterService.findById(ArgumentMatchers.any())).thenReturn(getConnectCluster());

        Mockito.when(connectClusterRest.getAllConnectorByClusterUrl(ArgumentMatchers.eq(CONNECT_CLUSTER_URL))).thenReturn(Lists.newArrayList());

        List<ConnectorInfoDTO> connectorInfos = Lists
                .newArrayList(new ConnectorInfoDTO(1L, CONNECT_CLUSTER_URL, connectorName, new HashMap<>(), ConnectorState.RUNTIME_FAILED, ConnectorState.RUNNING));

        Mockito.when(connectorCoreProcessService.queryConnector(
                ArgumentMatchers.eq(ConnectorState.RUNTIME_FAILED), ArgumentMatchers.eq(ConnectorState.RUNNING), ArgumentMatchers.eq(CONNECT_CLUSTER_ID)))
                .thenReturn(connectorInfos);

        connectorStateHandler.innerExtendEventWatcher(CONNECT_CLUSTER_ID);

        Map<Long, ConnectorStateMachine> stateMachineHolder = connectorStateHandler.getStateMachineHolder();
        Assert.assertEquals(ConnectorState.STARTING, stateMachineHolder.get(1L).getCurrentState());

        ArgumentCaptor<Long> connectorIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectorState> stateCaptor = ArgumentCaptor.forClass(ConnectorState.class);
        Mockito.verify(connectorCoreProcessService).updateActualState(connectorIdCaptor.capture(), stateCaptor.capture());
        Assert.assertEquals((Long) 1L, connectorIdCaptor.getValue());
        Assert.assertEquals(ConnectorState.STARTING, stateCaptor.getValue());
    }

    @Test
    public void testThrowCreatingConflictExceptionShouldTransitAsNormal() {
        Assert.assertEquals(6, connectorStateHandler.getUserTriggerEventHandlers().size());
        List<ConnectorInfoDTO> connectorInfos = Lists.newArrayList(new ConnectorInfoDTO(1L, CONNECT_CLUSTER_URL, "pendingToStarting", new HashMap<>(), ConnectorState.PENDING, ConnectorState.RUNNING));
        Mockito.when(connectorCoreProcessService.queryConnector(ArgumentMatchers.eq(ConnectorState.PENDING), ArgumentMatchers.eq(ConnectorState.RUNNING), ArgumentMatchers.eq(CONNECT_CLUSTER_ID)))
                .thenReturn(connectorInfos);

        ArgumentCaptor<String> connectClusterUrlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> connectorNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map> connectorConfigCaptor = ArgumentCaptor.forClass(Map.class);
        Mockito.doThrow(new HttpClientErrorException(HttpStatus.CONFLICT))
                .when(connectClusterRest).createConnector(connectClusterUrlCaptor.capture(), connectorNameCaptor.capture(), connectorConfigCaptor.capture());

        connectorStateHandler.getUserTriggerEventHandlers().forEach((event, handler) -> {
            handler.accept(CONNECT_CLUSTER_ID, event);
        });
        Map<Long, ConnectorStateMachine> stateMachineHolder = connectorStateHandler.getStateMachineHolder();
        Assert.assertEquals(ConnectorState.STARTING, stateMachineHolder.get(1L).getCurrentState());

        Assert.assertEquals(CONNECT_CLUSTER_URL, connectClusterUrlCaptor.getValue());
        Assert.assertEquals("pendingToStarting", connectorNameCaptor.getValue());
        Assert.assertEquals(new HashMap(), connectorConfigCaptor.getValue());

        ArgumentCaptor<Long> connectorIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConnectorState> stateCaptor = ArgumentCaptor.forClass(ConnectorState.class);
        Mockito.verify(connectorCoreProcessService).updateActualState(connectorIdCaptor.capture(), stateCaptor.capture());
        Assert.assertEquals((Long) 1L, connectorIdCaptor.getValue());
        Assert.assertEquals(ConnectorState.STARTING, stateCaptor.getValue());
    }

    @Test
    public void testThrowExceptionShouldResetToIdleState() {
        Assert.assertEquals(6, connectorStateHandler.getUserTriggerEventHandlers().size());
        List<ConnectorInfoDTO> connectorInfos = Lists.newArrayList(new ConnectorInfoDTO(1L, CONNECT_CLUSTER_URL, "pendingToStarting", new HashMap<>(), ConnectorState.PENDING, ConnectorState.RUNNING));
        Mockito.when(connectorCoreProcessService.queryConnector(ArgumentMatchers.eq(ConnectorState.PENDING), ArgumentMatchers.eq(ConnectorState.RUNNING), ArgumentMatchers.eq(CONNECT_CLUSTER_ID)))
                .thenReturn(connectorInfos);

        ArgumentCaptor<String> connectClusterUrlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> connectorNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map> connectorConfigCaptor = ArgumentCaptor.forClass(Map.class);
        Mockito.doThrow(new HttpClientErrorException(HttpStatus.BAD_REQUEST))
                .when(connectClusterRest).createConnector(connectClusterUrlCaptor.capture(), connectorNameCaptor.capture(), connectorConfigCaptor.capture());

        connectorStateHandler.getUserTriggerEventHandlers().forEach((event, handler) -> {
            handler.accept(CONNECT_CLUSTER_ID, event);
        });
        Map<Long, ConnectorStateMachine> stateMachineHolder = connectorStateHandler.getStateMachineHolder();
        Assert.assertEquals(ConnectorState.PENDING, stateMachineHolder.get(1L).getCurrentState());
        Assert.assertEquals(StateMachineStatus.IDLE, stateMachineHolder.get(1L).getStatus());

        Assert.assertEquals(CONNECT_CLUSTER_URL, connectClusterUrlCaptor.getValue());
        Assert.assertEquals("pendingToStarting", connectorNameCaptor.getValue());
        Assert.assertEquals(new HashMap(), connectorConfigCaptor.getValue());
    }

}
