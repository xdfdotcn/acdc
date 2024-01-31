package cn.xdf.acdc.devops.service.process.requisition;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourcePermissionRequisitionDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourcePermissionRequisitionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.core.domain.query.DataSystemResourcePermissionRequisitionQuery;

import java.util.List;

public interface DataSystemResourcePermissionRequisitionService {
    
    /**
     * Query resource permission requisition list with condition.
     *
     * @param query query object
     * @return query result
     */
    List<DataSystemResourcePermissionRequisitionDTO> query(DataSystemResourcePermissionRequisitionQuery query);
    
    /**
     * Get resource permission requisition by ID.
     *
     * @param id ID
     * @return DataSystemResourcePermissionRequisitionDTO
     */
    DataSystemResourcePermissionRequisitionDTO getById(Long id);
    
    /**
     * Get resource permission requisition detail by ID.
     *
     * @param id ID
     * @return DataSystemResourcePermissionRequisitionDetailDTO
     */
    DataSystemResourcePermissionRequisitionDetailDTO getDetailById(Long id);
    
    /**
     * Get requisition by third party ID .
     *
     * @param thirdPartyId thirdPartyId
     * @return DataSystemResourcePermissionRequisitionDTO
     */
    DataSystemResourcePermissionRequisitionDTO getByThirdPartyId(String thirdPartyId);
    
    /**
     * Get source Approval users.
     *
     * @param id id
     * @return UserDTO list
     */
    List<UserDTO> getSourceApprovalUsersById(Long id);
    
    /**
     * Get DBA Approval users.
     *
     * @return UserDTO list
     */
    List<UserDTO> getDbaApprovalUsers();
    
    /**
     * Get applicant by id .
     *
     * @param id id
     * @return UserDTO
     */
    UserDTO getApplicantById(Long id);
    
    /**
     * Update approve state.
     *
     * @param connectionRequisitionId connectionRequisitionId
     * @param state state
     */
    void updateApprovalState(Long connectionRequisitionId, ApprovalState state);
    
    /**
     * Update approval state by DBA.
     *
     * @param connectionRequisitionId connectionRequisitionId
     * @param state state
     * @param approveResult approveResult
     * @param dbaDomainAccount dbaDomainAccount
     */
    void updateApprovalStateByDBA(
            Long connectionRequisitionId,
            ApprovalState state,
            String approveResult,
            String dbaDomainAccount
    );
    
    /**
     * Update approval state by source owner.
     *
     * @param connectionRequisitionId connectionRequisitionId
     * @param state state
     * @param approveResult approveResult
     * @param sourceOwnerDomainAccount sourceOwnerDomainAccount
     */
    void updateApprovalStateBySourceOwner(
            Long connectionRequisitionId,
            ApprovalState state,
            String approveResult,
            String sourceOwnerDomainAccount
    );
    
    /**
     * Bind third party id, eg: OA.
     *
     * @param id id
     * @param thirdPartyId thirdPartyId
     */
    void bindThirdPartyId(Long id, String thirdPartyId);
    
}
