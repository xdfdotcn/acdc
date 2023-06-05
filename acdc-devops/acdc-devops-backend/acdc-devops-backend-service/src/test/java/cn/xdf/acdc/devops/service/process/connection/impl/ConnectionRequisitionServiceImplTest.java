package cn.xdf.acdc.devops.service.process.connection.impl;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionColumnConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionRequisitionDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionRequisitionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.dto.approve.ApproveDTO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.query.ConnectionRequisitionQuery;
import cn.xdf.acdc.devops.repository.DataSystemResourceRepository;
import cn.xdf.acdc.devops.repository.ProjectRepository;
import cn.xdf.acdc.devops.repository.UserRepository;
import cn.xdf.acdc.devops.service.error.exceptions.NotAuthorizedException;
import cn.xdf.acdc.devops.service.process.connection.ConnectionRequisitionService;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connection.approval.ApprovalStateMachine;
import cn.xdf.acdc.devops.service.process.connection.approval.event.ApprovalEventGenerator;
import cn.xdf.acdc.devops.service.process.user.UserService;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.EntityNotFoundException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class ConnectionRequisitionServiceImplTest {
    
    @Autowired
    private ConnectionService connectionService;
    
    @Autowired
    private ConnectionRequisitionService connectionRequisitionService;
    
    @Autowired
    private DataSystemResourceRepository dataSystemResourceRepository;
    
    @Autowired
    private ProjectRepository projectRepository;
    
    @Autowired
    private UserRepository userRepository;
    
    @Autowired
    private EntityManager entityManager;
    
    @MockBean
    private ApprovalStateMachine approvalStateMachine;
    
    @MockBean
    private UserService userService;
    
    @Mock
    private ApprovalEventGenerator approvalEventGenerator;
    
    private UserDO user;
    
    @Before
    public void setUp() {
        user = saveUser();
        when(approvalStateMachine.getApprovalEventGenerator()).thenReturn(approvalEventGenerator);
        when(userService.getByDomainAccount(eq(user.getDomainAccount()))).thenReturn(new UserDTO(user));
    }
    
    @Test
    public void testCreateRequisitionWithAutoSplitShouldGroupBySourceProjectId() {
        ConnectionRequisitionDetailDTO connectionRequisitionDetail = generateConnectionRequisitionDetail(user);
        List<ConnectionRequisitionDetailDTO> savedRequisitions = connectionRequisitionService.createRequisitionWithAutoSplit(connectionRequisitionDetail, user.getDomainAccount());
        
        // assert
        int savedConnectionCount = 0;
        // every connection in a requisition has same project id
        for (ConnectionRequisitionDetailDTO each : savedRequisitions) {
            Assertions.assertThat(each.getConnections().size()).isGreaterThan(1);
            savedConnectionCount += each.getConnections().size();
            Long sourceProjectId = null;
            for (ConnectionDetailDTO eachConnection : each.getConnections()) {
                if (sourceProjectId == null) {
                    sourceProjectId = eachConnection.getSourceProjectId();
                } else {
                    Assertions.assertThat(eachConnection.getSourceProjectId()).isEqualTo(sourceProjectId);
                }
            }
        }
        Assertions.assertThat(savedConnectionCount).isEqualTo(connectionRequisitionDetail.getConnections().size());
    }
    
    private ConnectionRequisitionDetailDTO generateConnectionRequisitionDetail(final UserDO user) {
        return new ConnectionRequisitionDetailDTO("connection_requisition", generateConnectionDetails(user, 10));
    }
    
    private List<ConnectionDetailDTO> generateConnectionDetails(final UserDO user, final int count) {
        List<ProjectDO> savedProjects = saveProjects(count, user);
        List<DataSystemResourceDO> savedResource = saveDataSystemResource(count);
        
        List<ConnectionDetailDTO> connectionDetails = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ConnectionDetailDTO connectionDetail = new ConnectionDetailDTO();
            connectionDetail.setUserId(user.getId());
            connectionDetail.setDesiredState(ConnectionState.STOPPED);
            connectionDetail.setActualState(ConnectionState.STOPPED);
            connectionDetail.setRequisitionState(RequisitionState.APPROVING);
            
            // source
            // for the test of requisition group
            connectionDetail.setSourceProjectId(savedProjects.get(i % 3).getId());
            connectionDetail.setSourceDataCollectionId(savedResource.get(i).getId());
            connectionDetail.setSourceDataSystemType(DataSystemType.MYSQL);
            
            // sink
            connectionDetail.setSinkProjectId(savedProjects.get(i).getId());
            connectionDetail.setSinkDataCollectionId(savedResource.get(i).getId());
            connectionDetail.setSinkDataSystemType(DataSystemType.MYSQL);
            connectionDetail.setSinkInstanceId(savedResource.get(i).getId());
            
            // configuration
            List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations = new ArrayList<>();
            ConnectionColumnConfigurationDTO connectionColumnConfiguration = new ConnectionColumnConfigurationDTO();
            connectionColumnConfiguration.setSourceColumnName("source_column");
            connectionColumnConfiguration.setSinkColumnName("sink_column");
            connectionColumnConfigurations.add(connectionColumnConfiguration);
            
            connectionDetail.setConnectionColumnConfigurations(connectionColumnConfigurations);
            
            connectionDetails.add(connectionDetail);
        }
        return connectionDetails;
    }
    
    private UserDO saveUser() {
        UserDO user = new UserDO();
        user.setEmail("user@acdc.io");
        user.setName("user");
        user.setDomainAccount("user");
        user.setPassword("user");
        user.setCreatedBy("user");
        return userRepository.save(user);
    }
    
    private List<ProjectDO> saveProjects(final int count, final UserDO owner) {
        List<ProjectDO> projects = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ProjectDO project = new ProjectDO();
            project.setName("project_" + i);
            project.setOwner(owner);
            projects.add(projectRepository.save(project));
        }
        return projects;
    }
    
    private List<DataSystemResourceDO> saveDataSystemResource(final int count) {
        List<DataSystemResourceDO> dataSystemResources = new ArrayList<>();
        
        DataSystemResourceDO cluster = new DataSystemResourceDO();
        cluster.setName("cluster");
        cluster.setDataSystemType(DataSystemType.MYSQL);
        cluster.setResourceType(DataSystemResourceType.MYSQL_CLUSTER);
        dataSystemResourceRepository.save(cluster);
        
        DataSystemResourceDO database = new DataSystemResourceDO();
        database.setName("database");
        database.setDataSystemType(DataSystemType.MYSQL);
        database.setResourceType(DataSystemResourceType.MYSQL_DATABASE);
        database.setParentResource(cluster);
        dataSystemResourceRepository.save(database);
        
        for (int i = 0; i < count; i++) {
            DataSystemResourceDO dataSystemResource = new DataSystemResourceDO();
            dataSystemResource.setName("data_system_resource_" + i);
            dataSystemResource.setDataSystemType(DataSystemType.MYSQL);
            dataSystemResource.setResourceType(DataSystemResourceType.MYSQL_TABLE);
            dataSystemResource.setParentResource(database);
            dataSystemResources.add(dataSystemResourceRepository.save(dataSystemResource));
        }
        return dataSystemResources;
    }
    
    @Test
    public void testQuery() {
        ConnectionRequisitionDetailDTO connectionRequisition = generateConnectionRequisitionDetail(user);
        List<ConnectionRequisitionDetailDTO> savedRequisitions = connectionRequisitionService.createRequisitionWithAutoSplit(connectionRequisition, user.getDomainAccount());
        
        // assert
        for (ConnectionRequisitionDetailDTO each : savedRequisitions) {
            for (ConnectionDetailDTO eachConnection : each.getConnections()) {
                ConnectionRequisitionQuery query = new ConnectionRequisitionQuery();
                query.setConnectionId(eachConnection.getId());
                List<ConnectionRequisitionDTO> queriedConnectionRequisitions = connectionRequisitionService.query(query);
                Assertions.assertThat(queriedConnectionRequisitions.size()).isEqualTo(1);
            }
        }
    }
    
    @Test
    public void testApproveRequisitionConnections() {
        ConnectionRequisitionDetailDTO connectionRequisition = generateConnectionRequisitionDetail(user);
        List<ConnectionRequisitionDetailDTO> savedRequisitions = connectionRequisitionService.createRequisitionWithAutoSplit(connectionRequisition, user.getDomainAccount());
        
        ConnectionRequisitionDetailDTO anyRequisition = savedRequisitions.get(0);
        connectionRequisitionService.approveRequisitionConnections(anyRequisition.getId());
        
        // assert
        anyRequisition.getConnections().forEach(each -> {
            ConnectionDTO connection = connectionService.getById(each.getId());
            Assertions.assertThat(connection.getRequisitionState()).isEqualTo(RequisitionState.APPROVED);
        });
    }
    
    @Test
    public void testApprove() {
        ConnectionRequisitionDetailDTO connectionRequisition = generateConnectionRequisitionDetail(user);
        List<ConnectionRequisitionDetailDTO> savedRequisitions = connectionRequisitionService.createRequisitionWithAutoSplit(connectionRequisition, user.getDomainAccount());
        
        ConnectionRequisitionDetailDTO anyRequisition = savedRequisitions.get(0);
        ApproveDTO approveDTO = new ApproveDTO();
        approveDTO.setApproved(Boolean.TRUE);
        approveDTO.setApproveResult("approve_result");
        connectionRequisitionService.approve(anyRequisition.getId(), approveDTO, user.getDomainAccount());
    }
    
    @Test
    public void testGetRequisitionByThirdPartyId() {
        ConnectionRequisitionDetailDTO connectionRequisition = generateConnectionRequisitionDetail(user);
        List<ConnectionRequisitionDetailDTO> savedRequisitions = connectionRequisitionService.createRequisitionWithAutoSplit(connectionRequisition, user.getDomainAccount());
        
        ConnectionRequisitionDetailDTO anyRequisition = savedRequisitions.get(0);
        connectionRequisitionService.bindThirdPartyId(anyRequisition.getId(), "third_party_id");
        ConnectionRequisitionDTO resultRequisition = connectionRequisitionService.getByThirdPartyId("third_party_id");
        
        Assertions.assertThat(resultRequisition.getId()).isEqualTo(anyRequisition.getId());
    }
    
    @Test
    public void testGetDetailById() {
        ConnectionRequisitionDetailDTO connectionRequisition = generateConnectionRequisitionDetail(user);
        List<ConnectionRequisitionDetailDTO> savedRequisitions = connectionRequisitionService.createRequisitionWithAutoSplit(connectionRequisition, user.getDomainAccount());
        
        ConnectionRequisitionDetailDTO anyRequisition = savedRequisitions.get(0);
        ConnectionRequisitionDetailDTO resultRequisition = connectionRequisitionService.getDetailById(anyRequisition.getId());
        
        Assertions.assertThat(resultRequisition.getId()).isEqualTo(anyRequisition.getId());
    }
    
    @Test(expected = EntityNotFoundException.class)
    public void testGetDetailByIdShouldThrowExceptionWhenNotFound() {
        connectionRequisitionService.getDetailById(1L);
    }
    
    @Test
    public void testGetById() {
        ConnectionRequisitionDetailDTO connectionRequisition = generateConnectionRequisitionDetail(user);
        List<ConnectionRequisitionDetailDTO> savedRequisitions = connectionRequisitionService.createRequisitionWithAutoSplit(connectionRequisition, user.getDomainAccount());
        
        ConnectionRequisitionDetailDTO anyRequisition = savedRequisitions.get(0);
        ConnectionRequisitionDTO resultRequisition = connectionRequisitionService.getById(anyRequisition.getId());
        
        Assertions.assertThat(resultRequisition.getId()).isEqualTo(anyRequisition.getId());
    }
    
    @Test(expected = EntityNotFoundException.class)
    public void testGetByIdByIdShouldThrowExceptionWhenNotFound() {
        connectionRequisitionService.getById(1L);
    }
    
    @Test
    public void testGetSourceOwners() {
        ConnectionRequisitionDetailDTO connectionRequisition = generateConnectionRequisitionDetail(user);
        List<ConnectionRequisitionDetailDTO> savedRequisitions = connectionRequisitionService.createRequisitionWithAutoSplit(connectionRequisition, user.getDomainAccount());
        
        ConnectionRequisitionDetailDTO anyRequisition = savedRequisitions.get(0);
        
        List<UserDTO> owners = connectionRequisitionService.getSourceOwners(anyRequisition.getId());
        Assertions.assertThat(owners.size()).isEqualTo(1);
        Assertions.assertThat(owners.get(0).getId()).isEqualTo(user.getId());
    }
    
    @Test(expected = EntityNotFoundException.class)
    public void testGetSourceOwnersShouldThrowExceptionWhenNotFound() {
        connectionRequisitionService.getSourceOwners(1L);
    }
    
    @Test
    public void testGetProposer() {
        ConnectionRequisitionDetailDTO connectionRequisition = generateConnectionRequisitionDetail(user);
        List<ConnectionRequisitionDetailDTO> savedRequisitions = connectionRequisitionService.createRequisitionWithAutoSplit(connectionRequisition, user.getDomainAccount());
        
        ConnectionRequisitionDetailDTO anyRequisition = savedRequisitions.get(0);
        
        UserDTO proposer = connectionRequisitionService.getProposer(anyRequisition.getId());
        Assertions.assertThat(proposer.getId()).isEqualTo(user.getId());
    }
    
    @Test
    public void testCheckSourceOwnerPermissionsShouldPassWhenUserIsSourceOwner() {
        ConnectionRequisitionDetailDTO connectionRequisition = generateConnectionRequisitionDetail(user);
        List<ConnectionRequisitionDetailDTO> savedRequisitions = connectionRequisitionService.createRequisitionWithAutoSplit(connectionRequisition, user.getDomainAccount());
        
        entityManager.flush();
        entityManager.clear();
        
        ConnectionRequisitionDetailDTO anyRequisition = savedRequisitions.get(0);
        
        when(userService.getByDomainAccount(anyString())).thenReturn(new UserDTO(user));
        connectionRequisitionService.checkSourceOwnerPermissions(anyRequisition.getId(), user.getDomainAccount());
    }
    
    @Test(expected = NotAuthorizedException.class)
    public void testCheckSourceOwnerPermissionsShouldErrorWhenUserIsNotSourceOwner() {
        ConnectionRequisitionDetailDTO connectionRequisition = generateConnectionRequisitionDetail(user);
        List<ConnectionRequisitionDetailDTO> savedRequisitions = connectionRequisitionService.createRequisitionWithAutoSplit(connectionRequisition, user.getDomainAccount());
        
        entityManager.flush();
        entityManager.clear();
        
        ConnectionRequisitionDetailDTO anyRequisition = savedRequisitions.get(0);
        
        UserDTO otherUser = new UserDTO();
        otherUser.setEmail("other_user");
        when(userService.getByDomainAccount(anyString())).thenReturn(otherUser);
        connectionRequisitionService.checkSourceOwnerPermissions(anyRequisition.getId(), user.getDomainAccount());
    }
    
    @Test
    public void testCheckDbaPermissionsShouldPassWhenUserIsDBA() {
        when(userService.isDBA(eq(user.getDomainAccount()))).thenReturn(Boolean.TRUE);
        connectionRequisitionService.checkDbaPermissions(user.getDomainAccount());
    }
    
    @Test(expected = NotAuthorizedException.class)
    public void testCheckDbaPermissionsShouldErrorWhenUserIsNotDBA() {
        when(userService.isDBA(eq(user.getDomainAccount()))).thenReturn(Boolean.FALSE);
        connectionRequisitionService.checkDbaPermissions(user.getDomainAccount());
    }
    
    @Test
    public void testCreateRequisition() {
        ConnectionRequisitionDetailDTO connectionRequisition = generateConnectionRequisitionDetail(user);
        connectionRequisitionService.create(connectionRequisition, user.getDomainAccount());
    }
    
    @Test
    public void testSaveRequisitionShouldGetCorrectProjectName() {
        ConnectionRequisitionDetailDTO connectionRequisition = generateConnectionRequisitionDetail(user);
        
        List<ConnectionDetailDTO> savedConnections = connectionService.batchCreate(connectionRequisition.getConnections(), user.getDomainAccount());
        connectionRequisition.setConnections(savedConnections);
        
        entityManager.flush();
        entityManager.clear();
        
        ConnectionRequisitionDetailDTO savedConnectionRequisitionDetail = ((ConnectionRequisitionServiceImpl) connectionRequisitionService).saveRequisition(connectionRequisition);
        
        ConnectionRequisitionDetailDTO resultConnectionRequisitionDetail = connectionRequisitionService.getDetailById(savedConnectionRequisitionDetail.getId());
        
        // to test after cascade insert, related DO's field still have correct value.
        Assertions.assertThat(resultConnectionRequisitionDetail.getConnections().get(0).getSinkProjectName()).isNotEmpty();
    }
    
    @Test
    public void testUpdateApproveStateBySourceOwner() {
        ConnectionRequisitionDetailDTO connectionRequisition = generateConnectionRequisitionDetail(user);
        List<ConnectionRequisitionDetailDTO> savedRequisitions = connectionRequisitionService.createRequisitionWithAutoSplit(connectionRequisition, user.getDomainAccount());
        
        ConnectionRequisitionDetailDTO anyRequisition = savedRequisitions.get(0);
        
        connectionRequisitionService.updateApproveStateBySourceOwner(anyRequisition.getId(), ApprovalState.DBA_APPROVING, "ok", user.getDomainAccount());
        
        // assert
        ConnectionRequisitionDTO resultRequisition = connectionRequisitionService.getById(anyRequisition.getId());
        Assertions.assertThat(resultRequisition.getState()).isEqualTo(ApprovalState.DBA_APPROVING);
        Assertions.assertThat(resultRequisition.getSourceApproveResult()).isEqualTo("ok");
    }
    
    @Test
    public void testUpdateApproveStateByDBA() {
        ConnectionRequisitionDetailDTO connectionRequisition = generateConnectionRequisitionDetail(user);
        List<ConnectionRequisitionDetailDTO> savedRequisitions = connectionRequisitionService.createRequisitionWithAutoSplit(connectionRequisition, user.getDomainAccount());
        
        ConnectionRequisitionDetailDTO anyRequisition = savedRequisitions.get(0);
        
        connectionRequisitionService.updateApproveStateByDBA(anyRequisition.getId(), ApprovalState.APPROVED, "ok", user.getDomainAccount());
        
        // assert
        ConnectionRequisitionDTO resultRequisition = connectionRequisitionService.getById(anyRequisition.getId());
        Assertions.assertThat(resultRequisition.getState()).isEqualTo(ApprovalState.APPROVED);
        Assertions.assertThat(resultRequisition.getDbaApproveResult()).isEqualTo("ok");
    }
    
    @Test
    public void testInvalidRequisition() {
        ConnectionRequisitionDetailDTO connectionRequisition = generateConnectionRequisitionDetail(user);
        List<ConnectionRequisitionDetailDTO> savedRequisitions = connectionRequisitionService.createRequisitionWithAutoSplit(connectionRequisition, user.getDomainAccount());
        
        ConnectionRequisitionDetailDTO anyRequisition = savedRequisitions.get(0);
        
        connectionRequisitionService.invalidRequisition(anyRequisition.getId());
        
        // assert
        for (ConnectionDetailDTO each : anyRequisition.getConnections()) {
            ConnectionDTO resultConnection = connectionService.getById(each.getId());
            Assertions.assertThat(resultConnection.isDeleted()).isTrue();
            Assertions.assertThat(resultConnection.getDesiredState()).isEqualTo(ConnectionState.STOPPED);
        }
    }
    
    @Test
    public void testBindThirdPartyId() {
        ConnectionRequisitionDetailDTO connectionRequisition = generateConnectionRequisitionDetail(user);
        List<ConnectionRequisitionDetailDTO> savedRequisitions = connectionRequisitionService.createRequisitionWithAutoSplit(connectionRequisition, user.getDomainAccount());
        
        ConnectionRequisitionDetailDTO anyRequisition = savedRequisitions.get(0);
        connectionRequisitionService.bindThirdPartyId(anyRequisition.getId(), "third_party_id");
        
        // assert
        ConnectionRequisitionDTO resultRequisition = connectionRequisitionService.getById(anyRequisition.getId());
        Assertions.assertThat(resultRequisition.getThirdPartyId()).isEqualTo("third_party_id");
    }
    
    @Test
    public void testUpdateApproveState() {
        ConnectionRequisitionDetailDTO connectionRequisition = generateConnectionRequisitionDetail(user);
        List<ConnectionRequisitionDetailDTO> savedRequisitions = connectionRequisitionService.createRequisitionWithAutoSplit(connectionRequisition, user.getDomainAccount());
        
        ConnectionRequisitionDetailDTO anyRequisition = savedRequisitions.get(0);
        connectionRequisitionService.updateApproveState(anyRequisition.getId(), ApprovalState.APPROVED);
        
        // assert
        ConnectionRequisitionDTO resultRequisition = connectionRequisitionService.getById(anyRequisition.getId());
        Assertions.assertThat(resultRequisition.getState()).isEqualTo(ApprovalState.APPROVED);
    }
}
