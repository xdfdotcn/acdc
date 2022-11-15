package cn.xdf.acdc.devops.service.process.connection;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionRequisitionDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionRequisitionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DomainUserDTO;
import cn.xdf.acdc.devops.core.domain.dto.approve.ApproveDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.core.domain.query.ConnectionQuery;

import java.util.List;

/**
 * Connection requisition service .
 */
public interface ConnectionRequisitionProcessService {

    /**
     * Requisition connection.
     *
     * @param requisition   requisition
     * @param domainAccount domainAccount
     */
    void bulkCreateRequisitionWithAutoSplit(ConnectionRequisitionDetailDTO requisition, String domainAccount);


    /**
     * 获取 connector 列表. ConnectionRequisitionController*
     *
     * @param query query
     * @return Page
     */
    List<ConnectionRequisitionDTO> query(ConnectionQuery query);

    /**
     * approve requisition.
     *
     * @param connectionRequisitionId connectionRequisitionId
     */
    void approveRequisitionConnections(Long connectionRequisitionId);

    /**
     * approve.
     *
     * @param connectionRequisitionId connectionRequisitionId
     * @param approveDTO              approveDTO
     * @param domainAccount           domainAccount
     */
    void approve(Long connectionRequisitionId, ApproveDTO approveDTO, String domainAccount);

    /**
     * Get requisition by third party ID .
     *
     * @param thirdPartyId thirdPartyId
     * @return ConnectionRequisitionDTO
     */
    ConnectionRequisitionDTO getRequisitionByThirdPartyId(String thirdPartyId);

    /**
     * Get Requisition detail.
     *
     * @param id id
     * @return requisition detail
     */
    ConnectionRequisitionDetailDTO getRequisitionDetail(Long id);

    /**
     * Get requisition.
     *
     * @param id id
     * @return Requisition
     */
    ConnectionRequisitionDTO getRequisition(Long id);


    /**
     * Query source owner email.
     *
     * @param id id
     * @return email list
     */
    List<DomainUserDTO> querySourceOwner(Long id);

    /**
     * Query proposer emails.
     *
     * @param id id
     * @return proposer email list
     */
    DomainUserDTO getProposer(Long id);

    /**
     * Check source owner permissions.
     *
     * @param domainAccount domainAccount
     * @param id            id
     */
    void checkSourceOwnerPermissions(Long id, String domainAccount);

    /**
     * Check dba permissions.
     *
     * @param id            id
     * @param domainAccount domainAccount
     */
    void checkDbaPermissions(Long id, String domainAccount);

    /**
     * Save requisition.
     *
     * @param requisitionDetail requisitionDetail
     * @return ConnectionRequisitionDTO
     */
    ConnectionRequisitionDTO save(ConnectionRequisitionDetailDTO requisitionDetail);


    /**
     * Create requisition.
     *
     * @param requisitionDetail requisitionDetail
     * @param domainAccount     domainAccount
     */
    void createRequisition(ConnectionRequisitionDetailDTO requisitionDetail, String domainAccount);

    /**
     * Update approve state.
     *
     * @param connectionRequisitionId connectionRequisitionId
     * @param state                   state
     */
    void updateApproveState(Long connectionRequisitionId, ApprovalState state);

    /**
     * Update approval state by DBA.
     *
     * @param connectionRequisitionId connectionRequisitionId
     * @param state                   state
     * @param approveResult           approveResult
     * @param dbaDomainAccount        dbaDomainAccount
     */
    void updateApproveStateByDBA(Long connectionRequisitionId, ApprovalState state, String approveResult, String dbaDomainAccount);

    /**
     * Update approval state by source owner.
     *
     * @param connectionRequisitionId  connectionRequisitionId
     * @param state                    state
     * @param approveResult            approveResult
     * @param sourceOwnerDomainAccount sourceOwnerDomainAccount
     */

    void updateApproveStateBySourceOwner(Long connectionRequisitionId, ApprovalState state, String approveResult, String sourceOwnerDomainAccount);

    /**
     * Bind third party id, eg: OA.
     *
     * @param id           id
     * @param thirdPartyId thirdPartyId
     */
    void bindThirdPartyId(Long id, String thirdPartyId);

    /**
     * Invalid requisition.
     *
     * @param id id
     */

    void invalidRequisition(Long id);
}
