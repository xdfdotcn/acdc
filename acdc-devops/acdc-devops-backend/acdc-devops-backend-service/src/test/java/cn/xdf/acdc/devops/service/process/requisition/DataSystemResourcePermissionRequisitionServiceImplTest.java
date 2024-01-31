package cn.xdf.acdc.devops.service.process.requisition;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourcePermissionRequisitionDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourcePermissionRequisitionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ProjectDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourcePermissionRequisitionDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.repository.DataSystemResourcePermissionRequisitionRepository;
import cn.xdf.acdc.devops.service.process.project.ProjectService;
import cn.xdf.acdc.devops.service.process.user.UserService;

@RunWith(SpringRunner.class)
public class DataSystemResourcePermissionRequisitionServiceImplTest {
    @Mock
    private UserService userService;
    
    @Mock
    private ProjectService projectService;
    
    @Mock
    private DataSystemResourcePermissionRequisitionRepository dataSystemResourcePermissionRequisitionRepository;
    
    // test objective
    private DataSystemResourcePermissionRequisitionServiceImpl permissionRequisitionService;
    
    @Before
    public void setUp() throws Exception {
        permissionRequisitionService = new DataSystemResourcePermissionRequisitionServiceImpl(
                dataSystemResourcePermissionRequisitionRepository
        );
        ReflectionTestUtils.setField(
                permissionRequisitionService,
                "dataSystemResourcePermissionRequisitionRepository",
                dataSystemResourcePermissionRequisitionRepository
        );
        ReflectionTestUtils.setField(permissionRequisitionService, "userService", userService);
        ReflectionTestUtils.setField(permissionRequisitionService, "projectService", projectService);
    }
    
    @Test
    public void testGetById() {
        when(dataSystemResourcePermissionRequisitionRepository.getOne(any())).thenReturn(createRequisitionDO());
        DataSystemResourcePermissionRequisitionDTO actual = permissionRequisitionService.getById(1L);
        DataSystemResourcePermissionRequisitionDTO expect = createRequisitionDTO(createRequisitionDO());
        Assertions.assertThat(actual).isEqualTo(expect);
    }
    
    @Test
    public void testGetDetailById() {
        when(dataSystemResourcePermissionRequisitionRepository.getOne(any())).thenReturn(createRequisitionDO());
        DataSystemResourcePermissionRequisitionDetailDTO actual = permissionRequisitionService.getDetailById(1L);
        DataSystemResourcePermissionRequisitionDetailDTO expect = createRequisitionDetailDTO(createRequisitionDO());
        Assertions.assertThat(actual).isEqualTo(expect);
    }
    
    @Test
    public void testGetByThirdPartyId() {
        when(dataSystemResourcePermissionRequisitionRepository.findByThirdPartyId(any()))
                .thenReturn(Optional.of(createRequisitionDO()));
        DataSystemResourcePermissionRequisitionDTO expect = createRequisitionDTO(createRequisitionDO());
        DataSystemResourcePermissionRequisitionDTO actual = permissionRequisitionService.getByThirdPartyId("test");
        Assertions.assertThat(actual).isEqualTo(expect);
    }
    
    @Test
    public void testGetSourceApprovalUsersById() {
        DataSystemResourcePermissionRequisitionDO requisitionDO = createRequisitionDO();
        UserDTO userDTO = new UserDTO()
                .setId(2L)
                .setName("test");
        when(dataSystemResourcePermissionRequisitionRepository.getOne(any()))
                .thenReturn(requisitionDO);
        when(projectService.getById(requisitionDO.getSourceProject().getId()))
                .thenReturn(new ProjectDTO(requisitionDO.getSourceProject()));
        when(userService.getById(requisitionDO.getSourceProject().getOwner().getId()))
                .thenReturn(userDTO);
        
        List<UserDTO> expect = Lists.newArrayList(userDTO);
        List<UserDTO> actual = permissionRequisitionService.getSourceApprovalUsersById(1L);
        
        Assertions.assertThat(actual).isEqualTo(expect);
    }
    
    @Test
    public void testGetDbaApprovalUsers() {
        List<UserDTO> users = Lists.newArrayList(
                new UserDTO().setId(2L).setName("test1"),
                new UserDTO().setId(2L).setName("test2")
        );
        when(userService.getDbaApprovalUsers()).thenReturn(users);
        
        List<UserDTO> expect = users;
        List<UserDTO> actual = permissionRequisitionService.getDbaApprovalUsers();
        
        Assertions.assertThat(actual).isEqualTo(expect);
    }
    
    @Test
    public void getApplicantById() {
        UserDTO userDTO = new UserDTO()
                .setId(2L)
                .setName("test");
        DataSystemResourcePermissionRequisitionDO requisitionDO = createRequisitionDO();
        when(dataSystemResourcePermissionRequisitionRepository.getOne(any()))
                .thenReturn(requisitionDO);
        
        when(userService.getById(requisitionDO.getUser().getId())).thenReturn(userDTO);
        
        UserDTO expect = userDTO;
        UserDTO actual = permissionRequisitionService.getApplicantById(1L);
        
        Assertions.assertThat(actual).isEqualTo(expect);
    }
    
    @Test
    public void testUpdateApprovalState() {
        DataSystemResourcePermissionRequisitionDO requisitionDO = createRequisitionDO();
        when(dataSystemResourcePermissionRequisitionRepository.getOne(any()))
                .thenReturn(requisitionDO);
        
        ApprovalState testState = ApprovalState.APPROVING;
        permissionRequisitionService.updateApprovalState(1L, testState);
        
        // verify
        ArgumentCaptor<DataSystemResourcePermissionRequisitionDO> captor = ArgumentCaptor.forClass(DataSystemResourcePermissionRequisitionDO.class);
        Mockito.verify(dataSystemResourcePermissionRequisitionRepository, Mockito.times(1))
                .save(captor.capture());
        DataSystemResourcePermissionRequisitionDO expect = createRequisitionDO().setState(testState);
        
        Assertions.assertThat(captor.getValue()).isEqualTo(expect);
    }
    
    @Test
    public void testUpdateApprovalStateByDBA() {
        UserDTO userDTO = new UserDTO()
                .setId(2L)
                .setName("dba1");
        DataSystemResourcePermissionRequisitionDO requisitionDO = createRequisitionDO();
        when(dataSystemResourcePermissionRequisitionRepository.getOne(any()))
                .thenReturn(requisitionDO);
        when(userService.getByDomainAccount("dba1")).thenReturn(userDTO);
        
        ApprovalState testState = ApprovalState.APPROVING;
        permissionRequisitionService.updateApprovalStateByDBA(1L, testState, "ok", "dba1");
        
        // verify
        ArgumentCaptor<DataSystemResourcePermissionRequisitionDO> captor = ArgumentCaptor.forClass(DataSystemResourcePermissionRequisitionDO.class);
        Mockito.verify(dataSystemResourcePermissionRequisitionRepository, Mockito.times(1))
                .save(captor.capture());
        
        DataSystemResourcePermissionRequisitionDO expect = createRequisitionDO().setState(testState).setDbaApprovalComments("ok").setDbaApproverUser(userDTO.toDO());
        
        Assertions.assertThat(captor.getValue()).isEqualTo(expect);
    }
    
    @Test
    public void testUpdateApprovalStateBySourceOwner() {
        UserDTO userDTO = new UserDTO()
                .setId(2L)
                .setName("source");
        DataSystemResourcePermissionRequisitionDO requisitionDO = createRequisitionDO();
        when(dataSystemResourcePermissionRequisitionRepository.getOne(any()))
                .thenReturn(requisitionDO);
        when(userService.getByDomainAccount("source")).thenReturn(userDTO);
        
        ApprovalState testState = ApprovalState.APPROVING;
        permissionRequisitionService.updateApprovalStateBySourceOwner(1L, testState, "pass", "source");
        
        // verify
        ArgumentCaptor<DataSystemResourcePermissionRequisitionDO> captor = ArgumentCaptor.forClass(DataSystemResourcePermissionRequisitionDO.class);
        Mockito.verify(dataSystemResourcePermissionRequisitionRepository, Mockito.times(1))
                .save(captor.capture());
        
        DataSystemResourcePermissionRequisitionDO expect = createRequisitionDO().setState(testState).setSourceApprovalComments("pass").setSourceApproverUser(userDTO.toDO());
        
        Assertions.assertThat(captor.getValue()).isEqualTo(expect);
    }
    
    @Test
    public void testBindThirdPartyId() {
        DataSystemResourcePermissionRequisitionDO requisitionDO = createRequisitionDO();
        String thirdPartyId = "123456";
        when(dataSystemResourcePermissionRequisitionRepository.getOne(any()))
                .thenReturn(requisitionDO);
        permissionRequisitionService.bindThirdPartyId(1L, thirdPartyId);
        
        // verify
        ArgumentCaptor<DataSystemResourcePermissionRequisitionDO> captor = ArgumentCaptor.forClass(DataSystemResourcePermissionRequisitionDO.class);
        Mockito.verify(dataSystemResourcePermissionRequisitionRepository, Mockito.times(1))
                .save(captor.capture());
        
        DataSystemResourcePermissionRequisitionDO expect = createRequisitionDO().setThirdPartyId(thirdPartyId);
        
        Assertions.assertThat(captor.getValue()).isEqualTo(expect);
    }
    
    private DataSystemResourcePermissionRequisitionDO createRequisitionDO() {
        UserDO applicant = new UserDO()
                .setId(11L)
                .setName("user00")
                .setDomainAccount("user00")
                .setEmail("user00@acdc.cn");
        
        UserDO sourceProjectOwner = new UserDO()
                .setId(11L)
                .setName("user01")
                .setDomainAccount("user01")
                .setEmail("user01@acdc.cn");
        
        UserDO sinkProjectOwner = new UserDO()
                .setId(11L)
                .setName("user02")
                .setDomainAccount("user02")
                .setEmail("user02@acdc.cn");
        
        UserDO sourceApproverUser = new UserDO()
                .setId(11L)
                .setName("user03")
                .setDomainAccount("user03")
                .setEmail("user03@acdc.cn");
        
        UserDO dbaApproverUser = new UserDO()
                .setId(11L)
                .setName("user04")
                .setDomainAccount("user04")
                .setEmail("user04@acdc.cn");
        
        Set<DataSystemResourceDO> dataSystemResources = Sets.newHashSet(
                new DataSystemResourceDO()
                        .setId(11L)
                        .setName("mysql_tb01")
                        .setDataSystemType(DataSystemType.MYSQL)
                        .setResourceType(DataSystemResourceType.MYSQL_TABLE)
                        .setParentResource(
                                new DataSystemResourceDO()
                                        .setId(12L)
                                        .setName("mysql_db01")
                                        .setDataSystemType(DataSystemType.MYSQL)
                                        .setResourceType(DataSystemResourceType.MYSQL_DATABASE)
                                        .setParentResource(
                                                new DataSystemResourceDO()
                                                        .setId(13L)
                                                        .setName("mysql_cluster01")
                                                        .setDataSystemType(DataSystemType.MYSQL)
                                                        .setResourceType(DataSystemResourceType.MYSQL_CLUSTER)
                                        )
                        ),
                
                new DataSystemResourceDO()
                        .setId(14L)
                        .setName("mysql_tb02")
                        .setDataSystemType(DataSystemType.MYSQL)
                        .setResourceType(DataSystemResourceType.MYSQL_TABLE)
                        .setParentResource(
                                new DataSystemResourceDO()
                                        .setId(15L)
                                        .setName("mysql_db02")
                                        .setDataSystemType(DataSystemType.MYSQL)
                                        .setResourceType(DataSystemResourceType.MYSQL_DATABASE)
                                        .setParentResource(
                                                new DataSystemResourceDO()
                                                        .setId(16L)
                                                        .setName("mysql_cluster02")
                                                        .setDataSystemType(DataSystemType.MYSQL)
                                                        .setResourceType(DataSystemResourceType.MYSQL_CLUSTER)
                                        )
                        )
        );
        
        ProjectDO sourceProject = new ProjectDO()
                .setId(11L)
                .setName("source_prj01")
                .setOwner(sourceProjectOwner);
        
        ProjectDO sinkProject = new ProjectDO()
                .setId(11L)
                .setName("sink_prj01")
                .setOwner(sinkProjectOwner);
        
        return new DataSystemResourcePermissionRequisitionDO()
                .setId(11L)
                .setThirdPartyId("6666")
                .setDescription("test")
                .setUser(applicant)
                .setSinkProject(sinkProject)
                .setSourceProject(sourceProject)
                .setSourceApproverUser(sourceApproverUser)
                .setSourceApprovalComments("source pass")
                .setDbaApproverUser(dbaApproverUser)
                .setDbaApprovalComments("dba pass")
                .setState(ApprovalState.APPROVED)
                .setDataSystemResources(dataSystemResources);
    }
    
    private DataSystemResourcePermissionRequisitionDTO createRequisitionDTO(final DataSystemResourcePermissionRequisitionDO requisitionDO) {
        return new DataSystemResourcePermissionRequisitionDTO(requisitionDO);
    }
    
    private DataSystemResourcePermissionRequisitionDetailDTO createRequisitionDetailDTO(final DataSystemResourcePermissionRequisitionDO requisitionDO) {
        return new DataSystemResourcePermissionRequisitionDetailDTO()
                .setId(requisitionDO.getId())
                .setThirdPartyId(requisitionDO.getThirdPartyId())
                .setDescription(requisitionDO.getDescription())
                .setState(requisitionDO.getState())
                .setUserId(requisitionDO.getUser().getId())
                .setUserDomainAccount(requisitionDO.getUser().getDomainAccount())
                .setSinkProjectName(requisitionDO.getSinkProject().getName())
                .setSinkProjectOwnerEmail(requisitionDO.getSinkProject().getOwner().getEmail())
                .setSourceProjectName(requisitionDO.getSourceProject().getName())
                .setDataSystemResources(requisitionDO
                        .getDataSystemResources()
                        .stream()
                        .map(DataSystemResourceDTO::new)
                        .collect(Collectors.toList())
                );
    }
}
