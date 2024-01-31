package cn.xdf.acdc.devops.service.process.requisition;

import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourcePermissionRequisitionBatchDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourcePermissionRequisitionDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.repository.DataSystemResourcePermissionRequisitionBatchRepository;
import cn.xdf.acdc.devops.repository.DataSystemResourcePermissionRequisitionRepository;
import cn.xdf.acdc.devops.repository.UserAuthorityRepository;
import cn.xdf.acdc.devops.repository.UserRepository;
import cn.xdf.acdc.devops.service.process.project.ProjectService;
import cn.xdf.acdc.devops.service.process.user.UserService;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.jts.util.Assert;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DataSystemResourcePermissionRequisitionBatchServiceImplTest {
    
    @MockBean
    private ProjectService projectService;
    
    @MockBean
    private UserService userService;
    
    @MockBean
    private UserAuthorityRepository userAuthorityRepository;
    
    @MockBean
    private UserRepository userRepository;
    
    @MockBean
    private DataSystemResourcePermissionRequisitionBatchRepository dataSystemResourcePermissionRequisitionBatchRepository;
    
    @MockBean
    private DataSystemResourcePermissionRequisitionRepository dataSystemResourcePermissionRequisitionRepository;
    
    @Autowired
    private DataSystemResourcePermissionRequisitionBatchServiceImpl requisitionBatchService;
    
    @Test
    public void testCreateShouldRelatedExistRequisition() {
        Mockito.when(dataSystemResourcePermissionRequisitionRepository.findBySourceProjectIdAndDataSystemResourceId(
                        ArgumentMatchers.eq(22L), ArgumentMatchers.eq(21L), ArgumentMatchers.eq(31L)))
                .thenReturn(Lists.newArrayList(
                                new DataSystemResourcePermissionRequisitionDO().setId(42L).setState(ApprovalState.DBA_REFUSED),
                                new DataSystemResourcePermissionRequisitionDO().setId(41L).setState(ApprovalState.APPROVING),
                                new DataSystemResourcePermissionRequisitionDO().setId(43L).setState(ApprovalState.SOURCE_OWNER_REFUSED)
                        )
                );
        ArgumentCaptor<Collection> requisitionsCaptor = ArgumentCaptor.forClass(Collection.class);
        Mockito.when(dataSystemResourcePermissionRequisitionRepository.saveAll(requisitionsCaptor.capture()))
                .thenReturn(Lists.newArrayList(new DataSystemResourcePermissionRequisitionDO().setId(51L)));
        
        ArgumentCaptor<DataSystemResourcePermissionRequisitionBatchDO> batchCaptor = ArgumentCaptor.forClass(DataSystemResourcePermissionRequisitionBatchDO.class);
        Mockito.when(dataSystemResourcePermissionRequisitionBatchRepository.save(batchCaptor.capture()))
                .thenReturn(new DataSystemResourcePermissionRequisitionBatchDO().setId(51L));
        Map<Long, Long> dataSystemResourceIdProjectIdMap = new HashMap<>();
        dataSystemResourceIdProjectIdMap.put(31L, 22L);
        Long batchId = requisitionBatchService.create(11L, "approve ", 21L, dataSystemResourceIdProjectIdMap);
        
        DataSystemResourcePermissionRequisitionDO resourcePermissionRequisition = (DataSystemResourcePermissionRequisitionDO) requisitionsCaptor.getValue().toArray()[0];
        Assert.equals(41L, resourcePermissionRequisition.getId());
        
        DataSystemResourcePermissionRequisitionBatchDO batchDO = batchCaptor.getValue();
        Assert.isTrue(batchDO.getId() == null);
        
        Assert.equals(51L, batchId);
    }
    
    @Test
    public void testCreateShouldCreateNewRequisitionWithoutRelatedOne() {
        Mockito.when(dataSystemResourcePermissionRequisitionRepository.findBySourceProjectIdAndDataSystemResourceId(
                        ArgumentMatchers.eq(22L), ArgumentMatchers.eq(21L), ArgumentMatchers.eq(31L)))
                .thenReturn(Lists.newArrayList(
                                new DataSystemResourcePermissionRequisitionDO().setId(42L).setState(ApprovalState.DBA_REFUSED),
                                new DataSystemResourcePermissionRequisitionDO().setId(43L).setState(ApprovalState.SOURCE_OWNER_REFUSED)
                        )
                );
        ArgumentCaptor<Collection> requisitionsCaptor = ArgumentCaptor.forClass(Collection.class);
        Mockito.when(dataSystemResourcePermissionRequisitionRepository.saveAll(requisitionsCaptor.capture()))
                .thenReturn(Lists.newArrayList(new DataSystemResourcePermissionRequisitionDO().setId(51L)));
        
        ArgumentCaptor<DataSystemResourcePermissionRequisitionBatchDO> batchCaptor = ArgumentCaptor.forClass(DataSystemResourcePermissionRequisitionBatchDO.class);
        Mockito.when(dataSystemResourcePermissionRequisitionBatchRepository.save(batchCaptor.capture()))
                .thenReturn(new DataSystemResourcePermissionRequisitionBatchDO().setId(51L));
        Map<Long, Long> dataSystemResourceIdProjectIdMap = new HashMap<>();
        dataSystemResourceIdProjectIdMap.put(31L, 22L);
        Long batchId = requisitionBatchService.create(11L, "approve ", 21L, dataSystemResourceIdProjectIdMap);
        
        DataSystemResourcePermissionRequisitionDO resourcePermissionRequisition = (DataSystemResourcePermissionRequisitionDO) requisitionsCaptor.getValue().toArray()[0];
        Assert.isTrue(resourcePermissionRequisition.getId() == null);
        
        DataSystemResourcePermissionRequisitionBatchDO batchDO = batchCaptor.getValue();
        Assert.isTrue(batchDO.getId() == null);
        
        Assert.equals(51L, batchId);
    }
    
    @Test
    public void testCreateShouldCreateNewRequisitionWithMultiDataSystemResourceWithoutRelatedOne() {
        Mockito.when(dataSystemResourcePermissionRequisitionRepository.findBySourceProjectIdAndDataSystemResourceId(
                ArgumentMatchers.eq(22L), ArgumentMatchers.eq(21L), ArgumentMatchers.eq(31L))
        ).thenReturn(Lists.newArrayList(
                new DataSystemResourcePermissionRequisitionDO().setId(42L).setState(ApprovalState.DBA_REFUSED),
                new DataSystemResourcePermissionRequisitionDO().setId(43L).setState(ApprovalState.SOURCE_OWNER_REFUSED)
        ));
        Mockito.when(dataSystemResourcePermissionRequisitionRepository.findBySourceProjectIdAndDataSystemResourceId(
                ArgumentMatchers.eq(22L), ArgumentMatchers.eq(21L), ArgumentMatchers.eq(32L))
        ).thenReturn(Lists.newArrayList(
                new DataSystemResourcePermissionRequisitionDO().setId(42L).setState(ApprovalState.DBA_REFUSED),
                new DataSystemResourcePermissionRequisitionDO().setId(43L).setState(ApprovalState.SOURCE_OWNER_REFUSED)
        ));
        ArgumentCaptor<Collection> requisitionsCaptor = ArgumentCaptor.forClass(Collection.class);
        Mockito.when(dataSystemResourcePermissionRequisitionRepository.saveAll(requisitionsCaptor.capture()))
                .thenReturn(Lists.newArrayList(new DataSystemResourcePermissionRequisitionDO().setId(51L)));
        
        ArgumentCaptor<DataSystemResourcePermissionRequisitionBatchDO> batchCaptor = ArgumentCaptor.forClass(DataSystemResourcePermissionRequisitionBatchDO.class);
        Mockito.when(dataSystemResourcePermissionRequisitionBatchRepository.save(batchCaptor.capture()))
                .thenReturn(new DataSystemResourcePermissionRequisitionBatchDO().setId(51L));
        Map<Long, Long> dataSystemResourceIdProjectIdMap = new HashMap<>();
        dataSystemResourceIdProjectIdMap.put(31L, 22L);
        dataSystemResourceIdProjectIdMap.put(32L, 22L);
        Long batchId = requisitionBatchService.create(11L, "approve ", 21L, dataSystemResourceIdProjectIdMap);
        
        DataSystemResourcePermissionRequisitionDO resourcePermissionRequisition = (DataSystemResourcePermissionRequisitionDO) requisitionsCaptor.getValue().toArray()[0];
        Assert.isTrue(resourcePermissionRequisition.getId() == null);
        Set<DataSystemResourceDO> dataSystemResources = resourcePermissionRequisition.getDataSystemResources();
        DataSystemResourceDO dataSystemResource0 = (DataSystemResourceDO) dataSystemResources.toArray()[0];
        Assert.equals(32L, dataSystemResource0.getId());
        DataSystemResourceDO dataSystemResource1 = (DataSystemResourceDO) dataSystemResources.toArray()[1];
        Assert.equals(31L, dataSystemResource1.getId());
        
        DataSystemResourcePermissionRequisitionBatchDO batchDO = batchCaptor.getValue();
        Assert.isTrue(batchDO.getId() == null);
        
        Assert.equals(51L, batchId);
    }
    
    @Configuration
    @ComponentScan(basePackages = "cn.xdf.acdc.devops.service.process.requisition")
    @EnableAspectJAutoProxy
    static class Config {
    
    }
}
