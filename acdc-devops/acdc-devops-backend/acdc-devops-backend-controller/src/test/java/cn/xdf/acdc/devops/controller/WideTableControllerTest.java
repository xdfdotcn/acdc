package cn.xdf.acdc.devops.controller;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourcePermissionRequisitionBatchDTO;
import cn.xdf.acdc.devops.core.domain.dto.widetable.WideTableDTO;
import cn.xdf.acdc.devops.core.domain.dto.widetable.WideTableDataSystemResourceProjectMappingDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalBatchState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.WideTableState;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.requisition.DataSystemResourcePermissionRequisitionBatchService;
import cn.xdf.acdc.devops.service.process.widetable.WideTableService;
import com.google.common.collect.Sets;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

@RunWith(MockitoJUnitRunner.class)
public class WideTableControllerTest {
    
    @Mock
    private WideTableService wideTableService;
    
    @Mock
    private ConnectionService connectionService;
    
    @Mock
    private DataSystemResourceService dataSystemResourceService;
    
    @Mock
    private DataSystemResourcePermissionRequisitionBatchService dataSystemResourcePermissionRequisitionBatchService;
    
    private WideTableController wideTableController;
    
    private final TaskScheduler taskScheduler = fakeTaskScheduler();
    
    @Before
    public void setup() {
        wideTableController = new WideTableController(wideTableService, connectionService,
                dataSystemResourcePermissionRequisitionBatchService, taskScheduler);
    }
    
    private TaskScheduler fakeTaskScheduler() {
        return new DefaultManagedTaskScheduler() {
            @Override
            public ScheduledFuture<?> scheduleAtFixedRate(final Runnable task, final Duration period) {
                task.run();
                return null;
            }
        };
    }
    
    @Test
    public void testApprovingWideTableWithNoneBatchIdShouldCreateRelatedRequisitionBatch() throws InterruptedException {
        Mockito.when(wideTableService.query(ArgumentMatchers.any())).thenReturn(fakeSingleApprovingWideTables());
        
        wideTableController.doStart();
        Thread.sleep(100L);
        
        ArgumentCaptor<WideTableDTO> captor = ArgumentCaptor.forClass(WideTableDTO.class);
        Mockito.verify(wideTableService).createWideTableRequisitionBatch(captor.capture());
        WideTableDTO captorValue = captor.getValue();
        Assert.assertEquals((Long) 1L, captorValue.getId());
    }
    
    @Test
    public void testRequisitionApprovedChangeWideTableState() throws InterruptedException {
        Mockito.when(wideTableService.query(ArgumentMatchers.any())).thenReturn(fakeApprovingWideTables());
        Mockito.when(dataSystemResourcePermissionRequisitionBatchService.query(ArgumentMatchers.any())).thenReturn(fakeRequisitionBatch());
        
        wideTableController.doStart();
        Thread.sleep(100L);
        ArgumentCaptor<Long> idCaptor = ArgumentCaptor.forClass(Long.class);
        Mockito.verify(wideTableService).createDataSystemResourceAndUpdateWideTable(idCaptor.capture());
        Mockito.verify(wideTableService).updateRequisitionStateToRefused(idCaptor.capture());
        
        List<Long> idValues = idCaptor.getAllValues();
        Assert.assertEquals((Long) 1L, idValues.get(0));
        Assert.assertEquals((Long) 2L, idValues.get(1));
    }
    
    @Test
    public void testWideTableDisableToRunningShouldCreateInnerConnections() throws InterruptedException {
        Mockito.when(wideTableService.query(ArgumentMatchers.any())).thenReturn(fakeApprovedWideTables());
        wideTableController.doStart();
        Thread.sleep(100L);
        ArgumentCaptor<Long> createIdCaptor = ArgumentCaptor.forClass(Long.class);
        Mockito.verify(wideTableService, Mockito.times(2)).startInnerConnectionAndUpdateActualStateToLoading(createIdCaptor.capture());
        
        List<Long> createIdValues = createIdCaptor.getAllValues();
        Assert.assertEquals((Long) 1L, createIdValues.get(0));
        Assert.assertEquals((Long) 2L, createIdValues.get(1));
    }
    
    @Test
    public void testWideTableStateShouldBeUpdatedToReadyWithConnectionsRunning() throws InterruptedException {
        Mockito.when(wideTableService.query(ArgumentMatchers.any())).thenReturn(fakeLoadingWideTables());
        Mockito.when(connectionService.query(ArgumentMatchers.any())).thenReturn(fakeRunningConnections());
        wideTableController.doStart();
        Thread.sleep(100L);
        ArgumentCaptor<Long> idCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<WideTableState> stateCaptor = ArgumentCaptor.forClass(WideTableState.class);
        Mockito.verify(wideTableService, Mockito.times(3)).updateWideTableActualState(idCaptor.capture(), stateCaptor.capture());
        
        Assert.assertEquals((Long) 1L, idCaptor.getValue());
        Assert.assertEquals(WideTableState.READY, stateCaptor.getValue());
    }
    
    @Test
    public void testWideTableStateShouldBeUpdatedToErrorWithConnectionsFailed() throws InterruptedException {
        Mockito.when(wideTableService.query(ArgumentMatchers.any())).thenReturn(fakeLoadingWideTables());
        Mockito.when(connectionService.query(ArgumentMatchers.any())).thenReturn(fakeFailedConnections());
        wideTableController.doStart();
        Thread.sleep(100L);
        ArgumentCaptor<Long> idCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<WideTableState> stateCaptor = ArgumentCaptor.forClass(WideTableState.class);
        Mockito.verify(wideTableService, Mockito.times(3)).updateWideTableActualState(idCaptor.capture(), stateCaptor.capture());
        
        Assert.assertEquals((Long) 1L, idCaptor.getValue());
        Assert.assertEquals(WideTableState.ERROR, stateCaptor.getValue());
    }
    
    @Test
    public void testWideTableStateShouldBeUpdatedToDisableWithDisabledDesiredState() throws InterruptedException {
        Mockito.when(wideTableService.query(ArgumentMatchers.any())).thenReturn(fakeWideTablesWithDisabledDesiredState());
        wideTableController.doStart();
        Thread.sleep(100L);
        ArgumentCaptor<Long> idCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<WideTableState> stateCaptor = ArgumentCaptor.forClass(WideTableState.class);
        Mockito.verify(wideTableService, Mockito.times(3)).updateWideTableActualState(idCaptor.capture(), stateCaptor.capture());
        List<Long> idValues = idCaptor.getAllValues();
        List<WideTableState> stateValues = stateCaptor.getAllValues();
        Assert.assertEquals((Long) 1L, idValues.get(0));
        Assert.assertEquals(WideTableState.DISABLED, stateValues.get(0));
        Assert.assertEquals((Long) 2L, idValues.get(1));
        Assert.assertEquals(WideTableState.DISABLED, stateValues.get(1));
        Assert.assertEquals((Long) 3L, idValues.get(2));
        Assert.assertEquals(WideTableState.DISABLED, stateValues.get(2));
    }
    
    private List<WideTableDTO> fakeWideTablesWithDisabledDesiredState() {
        List<WideTableDTO> result = new ArrayList<>();
        WideTableDTO wideTable1 = new WideTableDTO()
                .setId(1L)
                .setRequisitionState(RequisitionState.APPROVED)
                .setRequisitionBatchId(1L)
                .setRelatedConnectionIds(Sets.newHashSet(1L, 2L))
                .setUpdateTime(new Date())
                .setDesiredState(WideTableState.DISABLED)
                .setActualState(WideTableState.ERROR)
                .setUpdateTime(new Date())
                .setDeleted(false);
        result.add(wideTable1);
        WideTableDTO wideTable2 = new WideTableDTO()
                .setId(2L)
                .setRequisitionState(RequisitionState.APPROVED)
                .setRequisitionBatchId(2L)
                .setRelatedConnectionIds(Sets.newHashSet(3L, 4L))
                .setUpdateTime(new Date())
                .setDesiredState(WideTableState.DISABLED)
                .setActualState(WideTableState.READY)
                .setUpdateTime(new Date())
                .setDeleted(false);
        result.add(wideTable2);
        
        WideTableDTO wideTable3 = new WideTableDTO()
                .setId(3L)
                .setRequisitionState(RequisitionState.APPROVED)
                .setRequisitionBatchId(3L)
                .setRelatedConnectionIds(Sets.newHashSet(3L, 4L))
                .setUpdateTime(new Date())
                .setDesiredState(WideTableState.DISABLED)
                .setActualState(WideTableState.LOADING)
                .setUpdateTime(new Date())
                .setDeleted(false);
        result.add(wideTable3);
        
        WideTableDTO wideTable4 = new WideTableDTO()
                .setId(4L)
                .setRequisitionState(RequisitionState.APPROVED)
                .setRequisitionBatchId(3L)
                .setRelatedConnectionIds(Sets.newHashSet(3L, 4L))
                .setUpdateTime(new Date())
                .setDesiredState(WideTableState.DISABLED)
                .setActualState(WideTableState.DISABLED)
                .setUpdateTime(new Date())
                .setDeleted(false);
        result.add(wideTable4);
        return result;
    }
    
    private List<ConnectionDTO> fakeFailedConnections() {
        List<ConnectionDTO> result = new ArrayList<>();
        ConnectionDTO connection1 = new ConnectionDTO()
                .setId(1L)
                .setUpdateTime(new Date())
                .setDeleted(false)
                .setActualState(ConnectionState.FAILED);
        result.add(connection1);
        ConnectionDTO connection2 = new ConnectionDTO()
                .setId(2L)
                .setUpdateTime(new Date())
                .setDeleted(false)
                .setActualState(ConnectionState.STARTING);
        result.add(connection2);
        return result;
    }
    
    private List<ConnectionDTO> fakeRunningConnections() {
        List<ConnectionDTO> result = new ArrayList<>();
        ConnectionDTO connection1 = new ConnectionDTO()
                .setId(1L)
                .setUpdateTime(new Date())
                .setDeleted(false)
                .setActualState(ConnectionState.RUNNING);
        result.add(connection1);
        ConnectionDTO connection2 = new ConnectionDTO()
                .setId(2L)
                .setUpdateTime(new Date())
                .setDeleted(false)
                .setActualState(ConnectionState.RUNNING);
        result.add(connection2);
        ConnectionDTO connection3 = new ConnectionDTO()
                .setId(3L)
                .setUpdateTime(new Date())
                .setDeleted(false)
                .setActualState(ConnectionState.STARTING);
        result.add(connection3);
        ConnectionDTO connection4 = new ConnectionDTO()
                .setId(4L)
                .setUpdateTime(new Date())
                .setDeleted(false)
                .setActualState(ConnectionState.RUNNING);
        result.add(connection4);
        return result;
    }
    
    private List<WideTableDTO> fakeLoadingWideTables() {
        List<WideTableDTO> result = new ArrayList<>();
        WideTableDTO wideTable1 = new WideTableDTO()
                .setId(1L)
                .setRequisitionState(RequisitionState.APPROVED)
                .setRequisitionBatchId(1L)
                .setRelatedConnectionIds(Sets.newHashSet(1L, 2L))
                .setUpdateTime(new Date())
                .setDesiredState(WideTableState.READY)
                .setActualState(WideTableState.LOADING)
                .setUpdateTime(new Date())
                .setDeleted(false);
        result.add(wideTable1);
        WideTableDTO wideTable2 = new WideTableDTO()
                .setId(2L)
                .setRequisitionState(RequisitionState.APPROVED)
                .setRequisitionBatchId(2L)
                .setRelatedConnectionIds(Sets.newHashSet(3L, 4L))
                .setUpdateTime(new Date())
                .setDesiredState(WideTableState.READY)
                .setActualState(WideTableState.LOADING)
                .setUpdateTime(new Date())
                .setDeleted(false);
        result.add(wideTable2);
        return result;
    }
    
    private List<WideTableDTO> fakeApprovedWideTables() {
        List<WideTableDTO> result = new ArrayList<>();
        WideTableDTO wideTable1 = new WideTableDTO()
                .setId(1L)
                .setRequisitionState(RequisitionState.APPROVED)
                .setRequisitionBatchId(1L)
                .setRelatedConnectionIds(Sets.newHashSet(1L, 2L))
                .setUpdateTime(new Date())
                .setDesiredState(WideTableState.READY)
                .setActualState(WideTableState.DISABLED)
                .setUpdateTime(new Date())
                .setDeleted(false);
        result.add(wideTable1);
        WideTableDTO wideTable2 = new WideTableDTO()
                .setId(2L)
                .setRequisitionState(RequisitionState.APPROVED)
                .setRequisitionBatchId(2L)
                .setRelatedConnectionIds(Sets.newHashSet(3L, 4L))
                .setUpdateTime(new Date())
                .setDesiredState(WideTableState.READY)
                .setActualState(WideTableState.DISABLED)
                .setUpdateTime(new Date())
                .setDeleted(false);
        result.add(wideTable2);
        return result;
    }
    
    private List<WideTableDTO> fakeSingleApprovingWideTables() {
        List<WideTableDTO> result = new ArrayList<>();
        WideTableDTO wideTable1 = new WideTableDTO()
                .setId(1L)
                .setRequisitionState(RequisitionState.APPROVING)
                .setRelatedConnectionIds(Sets.newHashSet(1L, 2L))
                .setWideTableDataSystemResourceProjectMappings(
                        Sets.newHashSet(new WideTableDataSystemResourceProjectMappingDTO(1L, 1L, 30L, 40L),
                                new WideTableDataSystemResourceProjectMappingDTO(2L, 1L, 31L, 41L)
                        )
                )
                .setUpdateTime(new Date())
                .setDesiredState(WideTableState.READY)
                .setActualState(WideTableState.DISABLED)
                .setUserId(111L)
                .setSinkProjectId(222L)
                .setDescription("des")
                .setUpdateTime(new Date())
                .setDeleted(false);
        result.add(wideTable1);
        return result;
    }
    
    private List<WideTableDTO> fakeApprovingWideTables() {
        List<WideTableDTO> result = new ArrayList<>();
        WideTableDTO wideTable1 = new WideTableDTO()
                .setId(1L)
                .setRequisitionState(RequisitionState.APPROVING)
                .setRequisitionBatchId(1L)
                .setRelatedConnectionIds(Sets.newHashSet(1L, 2L))
                .setUpdateTime(new Date())
                .setDesiredState(WideTableState.READY)
                .setActualState(WideTableState.DISABLED)
                .setUpdateTime(new Date())
                .setDeleted(false);
        result.add(wideTable1);
        WideTableDTO wideTable2 = new WideTableDTO()
                .setId(2L)
                .setRequisitionState(RequisitionState.APPROVING)
                .setRequisitionBatchId(2L)
                .setRelatedConnectionIds(Sets.newHashSet(3L, 4L))
                .setUpdateTime(new Date())
                .setDesiredState(WideTableState.READY)
                .setActualState(WideTableState.DISABLED)
                .setUpdateTime(new Date())
                .setDeleted(false);
        result.add(wideTable2);
        return result;
    }
    
    private List<DataSystemResourcePermissionRequisitionBatchDTO> fakeRequisitionBatch() {
        List<DataSystemResourcePermissionRequisitionBatchDTO> result = new ArrayList<>();
        DataSystemResourcePermissionRequisitionBatchDTO batch1 = new DataSystemResourcePermissionRequisitionBatchDTO()
                .setId(1L)
                .setDeleted(false)
                .setState(ApprovalBatchState.APPROVED)
                .setUpdateTime(new Date());
        result.add(batch1);
        DataSystemResourcePermissionRequisitionBatchDTO batch2 = new DataSystemResourcePermissionRequisitionBatchDTO()
                .setId(2L)
                .setDeleted(false)
                .setState(ApprovalBatchState.REFUSED)
                .setUpdateTime(new Date());
        result.add(batch2);
        return result;
    }
    
}
