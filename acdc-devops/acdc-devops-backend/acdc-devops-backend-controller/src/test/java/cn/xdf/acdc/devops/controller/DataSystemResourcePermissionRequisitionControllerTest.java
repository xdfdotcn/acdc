package cn.xdf.acdc.devops.controller;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourcePermissionRequisitionBatchDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourcePermissionRequisitionDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalBatchState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.service.process.connection.approval.ApprovalStateMachine;
import cn.xdf.acdc.devops.service.process.requisition.DataSystemResourcePermissionRequisitionBatchService;
import cn.xdf.acdc.devops.service.process.requisition.DataSystemResourcePermissionRequisitionService;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.jts.util.Assert;
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
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

@RunWith(MockitoJUnitRunner.class)
public class DataSystemResourcePermissionRequisitionControllerTest {
    
    @Mock
    private DataSystemResourcePermissionRequisitionBatchService dataSystemResourcePermissionRequisitionBatchService;
    
    @Mock
    private DataSystemResourcePermissionRequisitionService dataSystemResourcePermissionRequisitionService;
    
    @Mock
    private ApprovalStateMachine approvalStateMachine;
    
    private final TaskScheduler taskScheduler = fakeTaskScheduler();
    
    private DataSystemResourcePermissionRequisitionController requisitionController;
    
    @Before
    public void setup() {
        requisitionController = new DataSystemResourcePermissionRequisitionController(
                dataSystemResourcePermissionRequisitionBatchService,
                dataSystemResourcePermissionRequisitionService,
                taskScheduler,
                approvalStateMachine
        );
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
    public void testBatchPendingShouldAutoToApproving() throws InterruptedException {
        Mockito.when(dataSystemResourcePermissionRequisitionBatchService.query(ArgumentMatchers.any())).thenReturn(fakePendingRequisitionBatch());
        
        requisitionController.doStart();
        Thread.sleep(100L);
        
        ArgumentCaptor<Long> batchIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ApprovalBatchState> stateCaptor = ArgumentCaptor.forClass(ApprovalBatchState.class);
        Mockito.verify(dataSystemResourcePermissionRequisitionBatchService).updateState(batchIdCaptor.capture(), stateCaptor.capture());
        Assert.equals(1L, batchIdCaptor.getValue());
        Assert.equals(ApprovalBatchState.APPROVING, stateCaptor.getValue());
    }
    
    @Test
    public void testBatchApprovingToRefusedShouldAsExpected() throws InterruptedException {
        Mockito.when(dataSystemResourcePermissionRequisitionBatchService.query(ArgumentMatchers.any())).thenReturn(fakeApprovingRequisitionBatch());
        Mockito.when(dataSystemResourcePermissionRequisitionService.query(ArgumentMatchers.any())).thenReturn(fakeRefusedRequisition());
        
        requisitionController.doStart();
        Thread.sleep(100L);
        
        ArgumentCaptor<Long> batchIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ApprovalBatchState> stateCaptor = ArgumentCaptor.forClass(ApprovalBatchState.class);
        Mockito.verify(dataSystemResourcePermissionRequisitionBatchService, Mockito.times(2)).updateState(batchIdCaptor.capture(), stateCaptor.capture());
        Assert.equals(1L, batchIdCaptor.getValue());
        Assert.equals(ApprovalBatchState.REFUSED, stateCaptor.getValue());
    }
    
    @Test
    public void testBatchApprovingToApprovedShouldAsExpected() throws InterruptedException {
        Mockito.when(dataSystemResourcePermissionRequisitionBatchService.query(ArgumentMatchers.any())).thenReturn(fakeApprovingRequisitionBatch());
        Mockito.when(dataSystemResourcePermissionRequisitionService.query(ArgumentMatchers.any())).thenReturn(fakeApprovedRequisition());
        
        requisitionController.doStart();
        Thread.sleep(100L);
        
        ArgumentCaptor<Long> batchIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ApprovalBatchState> stateCaptor = ArgumentCaptor.forClass(ApprovalBatchState.class);
        Mockito.verify(dataSystemResourcePermissionRequisitionBatchService, Mockito.times(3)).updateState(batchIdCaptor.capture(), stateCaptor.capture());
        Assert.equals(1L, batchIdCaptor.getValue());
        Assert.equals(ApprovalBatchState.APPROVED, stateCaptor.getValue());
    }
    
    private List<DataSystemResourcePermissionRequisitionDTO> fakeApprovedRequisition() {
        List<DataSystemResourcePermissionRequisitionDTO> result = new ArrayList<>();
        DataSystemResourcePermissionRequisitionDTO requisition1 = new DataSystemResourcePermissionRequisitionDTO()
                .setId(20L)
                .setState(ApprovalState.APPROVED)
                .setDeleted(false)
                .setUpdateTime(new Date());
        result.add(requisition1);
        DataSystemResourcePermissionRequisitionDTO requisition2 = new DataSystemResourcePermissionRequisitionDTO()
                .setId(21L)
                .setState(ApprovalState.APPROVED)
                .setDeleted(false)
                .setUpdateTime(new Date());
        result.add(requisition2);
        return result;
    }
    
    private List<DataSystemResourcePermissionRequisitionDTO> fakeRefusedRequisition() {
        List<DataSystemResourcePermissionRequisitionDTO> result = new ArrayList<>();
        DataSystemResourcePermissionRequisitionDTO requisition1 = new DataSystemResourcePermissionRequisitionDTO()
                .setId(20L)
                .setState(ApprovalState.DBA_APPROVING)
                .setDeleted(false)
                .setUpdateTime(new Date());
        result.add(requisition1);
        DataSystemResourcePermissionRequisitionDTO requisition2 = new DataSystemResourcePermissionRequisitionDTO()
                .setId(21L)
                .setState(ApprovalState.SOURCE_OWNER_REFUSED)
                .setDeleted(false)
                .setUpdateTime(new Date());
        result.add(requisition2);
        return result;
    }
    
    private List<DataSystemResourcePermissionRequisitionBatchDTO> fakeApprovingRequisitionBatch() {
        List<DataSystemResourcePermissionRequisitionBatchDTO> result = new ArrayList<>();
        DataSystemResourcePermissionRequisitionBatchDTO batchDTO = new DataSystemResourcePermissionRequisitionBatchDTO()
                .setId(1L)
                .setState(ApprovalBatchState.APPROVING)
                .setDataSystemResourcePermissionRequisitionIds(Sets.newHashSet(20L, 21L))
                .setDeleted(false)
                .setUpdateTime(new Date());
        result.add(batchDTO);
        return result;
    }
    
    private List<DataSystemResourcePermissionRequisitionBatchDTO> fakePendingRequisitionBatch() {
        List<DataSystemResourcePermissionRequisitionBatchDTO> result = new ArrayList<>();
        DataSystemResourcePermissionRequisitionBatchDTO batchDTO = new DataSystemResourcePermissionRequisitionBatchDTO()
                .setId(1L)
                .setState(ApprovalBatchState.PENDING)
                .setDataSystemResourcePermissionRequisitionIds(new HashSet<>())
                .setDeleted(false)
                .setUpdateTime(new Date());
        result.add(batchDTO);
        return result;
    }
}
