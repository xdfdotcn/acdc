package cn.xdf.acdc.devops.service.process.requisition;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourcePermissionRequisitionDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourcePermissionRequisitionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourcePermissionRequisitionDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.core.domain.query.DataSystemResourcePermissionRequisitionQuery;
import cn.xdf.acdc.devops.repository.DataSystemResourcePermissionRequisitionRepository;
import cn.xdf.acdc.devops.service.process.connection.approval.error.ApprovalProcessStateMatchErrorException;
import cn.xdf.acdc.devops.service.process.project.ProjectService;
import cn.xdf.acdc.devops.service.process.user.UserService;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Transactional
@Service
public class DataSystemResourcePermissionRequisitionServiceImpl implements DataSystemResourcePermissionRequisitionService {
    
    @Autowired
    private UserService userService;
    
    @Autowired
    private ProjectService projectService;
    
    private final DataSystemResourcePermissionRequisitionRepository dataSystemResourcePermissionRequisitionRepository;
    
    public DataSystemResourcePermissionRequisitionServiceImpl(
            final DataSystemResourcePermissionRequisitionRepository dataSystemResourcePermissionRequisitionRepository
    ) {
        this.dataSystemResourcePermissionRequisitionRepository = dataSystemResourcePermissionRequisitionRepository;
    }
    
    @Override
    public List<DataSystemResourcePermissionRequisitionDTO> query(final DataSystemResourcePermissionRequisitionQuery query) {
        return dataSystemResourcePermissionRequisitionRepository.query(query)
                .stream()
                .map(DataSystemResourcePermissionRequisitionDTO::new)
                .collect(Collectors.toList());
    }
    
    @Override
    public DataSystemResourcePermissionRequisitionDTO getById(final Long id) {
        return new DataSystemResourcePermissionRequisitionDTO(
                dataSystemResourcePermissionRequisitionRepository.getOne(id)
        );
    }
    
    @Override
    public DataSystemResourcePermissionRequisitionDetailDTO getDetailById(final Long id) {
        return new DataSystemResourcePermissionRequisitionDetailDTO(
                dataSystemResourcePermissionRequisitionRepository.getOne(id)
        );
    }
    
    @Override
    public DataSystemResourcePermissionRequisitionDTO getByThirdPartyId(final String thirdPartyId) {
        return dataSystemResourcePermissionRequisitionRepository.findByThirdPartyId(thirdPartyId)
                .map(DataSystemResourcePermissionRequisitionDTO::new)
                .orElseThrow(() -> new ApprovalProcessStateMatchErrorException(String
                        .format("Not found entity, thirdPartyId: %s", thirdPartyId)
                ));
    }
    
    @Override
    public List<UserDTO> getSourceApprovalUsersById(final Long id) {
        Long sourceProjectId = getById(id).getSourceProjectId();
        Long sourceProjectOwnerId = projectService.getById(sourceProjectId).getOwnerId();
        
        return Lists.newArrayList(userService.getById(sourceProjectOwnerId));
    }
    
    @Override
    public List<UserDTO> getDbaApprovalUsers() {
        return userService.getDbaApprovalUsers();
    }
    
    @Override
    public UserDTO getApplicantById(final Long id) {
        return userService.getById(getById(id).getUserId());
    }
    
    @Override
    public void updateApprovalState(
            final Long connectionRequisitionId,
            final ApprovalState state
    ) {
        DataSystemResourcePermissionRequisitionDO requisition = dataSystemResourcePermissionRequisitionRepository
                .getOne(connectionRequisitionId);
        requisition.setState(state);
        
        dataSystemResourcePermissionRequisitionRepository.save(requisition);
    }
    
    @Override
    public void updateApprovalStateByDBA(
            final Long connectionRequisitionId,
            final ApprovalState state,
            final String approveResult,
            final String dbaDomainAccount
    ) {
        UserDTO user = userService.getByDomainAccount(dbaDomainAccount);
        DataSystemResourcePermissionRequisitionDO requisition = dataSystemResourcePermissionRequisitionRepository
                .getOne(connectionRequisitionId);
        requisition.setState(state);
        requisition.setDbaApprovalComments(approveResult);
        requisition.setDbaApproverUser(user.toDO());
        
        dataSystemResourcePermissionRequisitionRepository.save(requisition);
    }
    
    @Override
    public void updateApprovalStateBySourceOwner(
            final Long connectionRequisitionId,
            final ApprovalState state,
            final String approveResult,
            final String sourceOwnerDomainAccount
    ) {
        UserDTO user = userService.getByDomainAccount(sourceOwnerDomainAccount);
        
        DataSystemResourcePermissionRequisitionDO requisition = dataSystemResourcePermissionRequisitionRepository
                .getOne(connectionRequisitionId);
        requisition.setState(state);
        requisition.setSourceApprovalComments(approveResult);
        requisition.setSourceApproverUser(user.toDO());
        
        dataSystemResourcePermissionRequisitionRepository.save(requisition);
    }
    
    @Override
    public void bindThirdPartyId(final Long id, final String thirdPartyId) {
        DataSystemResourcePermissionRequisitionDO requisition = dataSystemResourcePermissionRequisitionRepository
                .getOne(id);
        
        requisition.setThirdPartyId(thirdPartyId);
        
        dataSystemResourcePermissionRequisitionRepository.save(requisition);
    }
}
