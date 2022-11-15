package cn.xdf.acdc.devops.service.process.connection.impl;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionRequisitionConnectionMappingDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionRequisitionDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionRequisitionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DomainUserDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.dto.approve.ApproveDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionRequisitionConnectionMappingDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionRequisitionDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.query.ConnectionQuery;
import cn.xdf.acdc.devops.core.domain.query.ConnectionRequisitionConnectionMappingQuery;
import cn.xdf.acdc.devops.core.domain.query.ConnectionRequisitionQuery;
import cn.xdf.acdc.devops.repository.ConnectionColumnConfigurationRepository;
import cn.xdf.acdc.devops.repository.ConnectionRepository;
import cn.xdf.acdc.devops.repository.ConnectionRequisitionConnectionMappingRepository;
import cn.xdf.acdc.devops.repository.ConnectionRequisitionRepository;
import cn.xdf.acdc.devops.repository.ProjectRepository;
import cn.xdf.acdc.devops.repository.UserRepository;
import cn.xdf.acdc.devops.service.error.ErrorMsg.Authorization;
import cn.xdf.acdc.devops.service.process.connection.ConnectionProcessService;
import cn.xdf.acdc.devops.service.process.connection.ConnectionRequisitionConnectionMappingProcessService;
import cn.xdf.acdc.devops.service.process.connection.ConnectionRequisitionProcessService;
import cn.xdf.acdc.devops.service.process.connection.approval.ApprovalContext;
import cn.xdf.acdc.devops.service.process.connection.approval.ApprovalStateMachine;
import cn.xdf.acdc.devops.service.process.connection.approval.error.ApprovalProcessStateMatchErrorException;
import cn.xdf.acdc.devops.service.process.connection.approval.event.ApprovalEvent;
import cn.xdf.acdc.devops.service.process.connection.approval.event.ApprovalOperation;
import cn.xdf.acdc.devops.service.process.connection.fieldmapping.impl.FieldMappingProcessServiceManager;
import cn.xdf.acdc.devops.service.process.datasystem.DatasetProcessServiceManager;
import cn.xdf.acdc.devops.service.process.project.ProjectProcessService;
import cn.xdf.acdc.devops.service.process.user.UserProcessService;
import cn.xdf.acdc.devops.service.util.BizAssert;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.persistence.EntityNotFoundException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class ConnectionRequisitionProcessServiceImpl implements ConnectionRequisitionProcessService {

    @Autowired
    private ConnectionProcessService connectionProcessService;

    @Autowired
    private ConnectionRequisitionProcessService connectionRequisitionProcessService;

    @Autowired
    private ConnectionRequisitionRepository connectionRequisitionRepository;

    @Autowired
    private ConnectionRepository connectionRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private ProjectRepository projectRepository;

    @Autowired
    private ProjectProcessService projectProcessService;

    @Autowired
    private DatasetProcessServiceManager datasetManager;

    @Autowired
    private ConnectionRequisitionConnectionMappingRepository connectionRequisitionConnectionMappingRepository;

    @Autowired
    private ConnectionColumnConfigurationRepository connectionColumnConfigurationRepository;

    @Autowired
    private FieldMappingProcessServiceManager fieldMappingManager;

    @Autowired
    private UserProcessService userProcessService;

    @Autowired
    private ConnectionRequisitionConnectionMappingProcessService connectionRequisitionConnectionMappingProcessService;

    @Autowired
    private ApprovalStateMachine approvalStateMachine;

    @Value("${acdc.approval.base-url:''}")
    private String baseUrl;

    @Override
    public void bulkCreateRequisitionWithAutoSplit(final ConnectionRequisitionDetailDTO requisition, final String domainAccount) {
        BizAssert.badRequest(!CollectionUtils.isEmpty(requisition.getConnections()),
                "Connections must not be empty.",
                String.format("Connections must not be empty: %s", requisition));

        List<ConnectionRequisitionDetailDTO> requisitions = splitRequisition(requisition);

        // TODO 1. 每个 requisition 的处理都在不同的事务中, 临时解决bug:批量提交的时候,因为某个 requisition 处理失败,导致事务的全部回滚
        // TODO 2. 目前 requisition 是按照项目维度进行拆分的, 如果批量中存在失败的 requisition, 则重新发起请求重试,
        // TODO 3. 方法级别的事务,会覆盖类级别的事务,方法需要声明为public才会被spring代理
        // TODO 4. 临时解决事务代理不生效的问题,注入一个本类的service对象
        requisitions.forEach(it -> connectionRequisitionProcessService.createRequisition(it, domainAccount));
    }

    @Override
    @Transactional
    public List<ConnectionRequisitionDTO> query(final ConnectionQuery query) {

        List<ConnectionRequisitionConnectionMappingDO> mappings = connectionRequisitionConnectionMappingRepository
                .query(ConnectionRequisitionConnectionMappingQuery.builder().connectionId(query.getConnectionId()).build());

        List<Long> requisitionIds =
                mappings.stream().map(each -> each.getConnectionRequisition().getId()).collect(Collectors.toList());

        if (CollectionUtils.isEmpty(requisitionIds)) {
            return Collections.EMPTY_LIST;
        }

        ConnectionRequisitionQuery connectionRequisitionQuery = ConnectionRequisitionQuery.builder()
                .connectionRequisitionIds(requisitionIds).build();

        return connectionRequisitionRepository.query(connectionRequisitionQuery)
                .stream().map(it -> {
                    String sourceApproverEmail = Objects.isNull(it.getSourceApproverUser())
                            ? SystemConstant.EMPTY_STRING
                            : it.getSourceApproverUser().getEmail();

                    String dbaApproverEmail = Objects.isNull(it.getDbaApproverUser())
                            ? SystemConstant.EMPTY_STRING
                            : it.getDbaApproverUser().getEmail();
                    return ConnectionRequisitionDTO.toConnectionRequisitionDTO(
                            it,
                            sourceApproverEmail,
                            dbaApproverEmail
                    );
                }).collect(Collectors.toList());
    }

    @Override
    @Transactional
    public void approveRequisitionConnections(final Long id) {
        List<ConnectionRequisitionConnectionMappingDO> mappings = connectionRequisitionConnectionMappingRepository
                .query(ConnectionRequisitionConnectionMappingQuery.builder().connectionRequisitionId(id).build());

        List<Long> connectionIds = mappings.stream().map(it -> it.getConnection().getId()).collect(Collectors.toList());

        ConnectionQuery connectionQuery = ConnectionQuery.builder()
                .requisitionState(RequisitionState.APPROVING)
                .connectionIds(connectionIds)
                .build();

        connectionProcessService.bulkEditConnectionRequisitionStateByQuery(connectionQuery, RequisitionState.APPROVED);
    }

    @Override
    @Transactional
    public void approve(final Long connectionRequisitionId, final ApproveDTO approveDTO, final String domainAccount) {
        ApprovalOperation operation = approveDTO.getApproved() ? ApprovalOperation.PASS : ApprovalOperation.REFUSED;
        String approveResult = approveDTO.getApproveResult();
        ApprovalEvent event = approvalStateMachine.getApprovalEventGenerator().generateApprovalEvent(connectionRequisitionId, operation);
        ApprovalContext context = ApprovalContext.builder().id(connectionRequisitionId).operatorId(domainAccount).description(approveResult).build();
        approvalStateMachine.fire(event, context);
    }

    @Override
    public ConnectionRequisitionDTO getRequisitionByThirdPartyId(final String thirdPartyId) {
        return connectionRequisitionRepository.findByThirdPartyId(thirdPartyId)
                .map(ConnectionRequisitionDTO::toConnectionRequisitionDTO)
                .orElseThrow(() -> new ApprovalProcessStateMatchErrorException(String.format("Not found entity, thirdPartyId: %s", thirdPartyId)));
    }

    private List<ConnectionRequisitionDetailDTO> splitRequisition(final ConnectionRequisitionDetailDTO requisitionDTO) {
        List<ConnectionDetailDTO> connectionDetailDTOs = requisitionDTO.getConnections();

        Map<Long, List<ConnectionDetailDTO>> groupedConnection = connectionDetailDTOs.stream()
                .collect(Collectors.groupingBy(ConnectionDetailDTO::getSourceProjectId));

        return groupedConnection.entrySet().stream()
                .map(it -> ConnectionRequisitionDetailDTO.of(requisitionDTO.getDescription(), it.getValue()))
                .collect(Collectors.toList());
    }

    private void saveRequisitionAndConnectionMapping(
            final List<ConnectionDTO> connections,
            final ConnectionRequisitionDTO requisition) {
        List<ConnectionRequisitionConnectionMappingDO> mappings = connections.stream()
                .map(it -> ConnectionRequisitionConnectionMappingDTO
                        .toConnectionRequisitionConnectionMappingDO(requisition, it)
                ).collect(Collectors.toList());

        connectionRequisitionConnectionMappingProcessService.saveAll(mappings);
    }

    @Override
    @Transactional
    public ConnectionRequisitionDetailDTO getRequisitionDetail(final Long id) {
        ConnectionRequisitionDO requisitionDO = connectionRequisitionRepository.findById(id)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", id)));

        List<Long> connectionIds = getConnectionIdsByRequisitionId(requisitionDO.getId());
        ConnectionQuery connectionQuery = ConnectionQuery.builder()
                .connectionIds(connectionIds).build();
        List<ConnectionDetailDTO> connectionDetails = connectionProcessService.detailQuery(connectionQuery);

        String sourceApproverEmail = Objects.isNull(requisitionDO.getSourceApproverUser())
                ? SystemConstant.EMPTY_STRING
                : requisitionDO.getSourceApproverUser().getEmail();

        String dbaApproverEmail = Objects.isNull(requisitionDO.getDbaApproverUser())
                ? SystemConstant.EMPTY_STRING
                : requisitionDO.getDbaApproverUser().getEmail();

        ConnectionRequisitionDetailDTO requisitionDetail = ConnectionRequisitionDetailDTO.toConnectionRequisitionDetailDTO(
                requisitionDO,
                connectionDetails,
                sourceApproverEmail,
                dbaApproverEmail
        );

        appendRequisitionLinkUrl(requisitionDetail);

        return requisitionDetail;
    }

    @Override
    @Transactional
    public ConnectionRequisitionDTO getRequisition(final Long id) {
        return connectionRequisitionRepository.findById(id)
                .map(ConnectionRequisitionDTO::toConnectionRequisitionDTO)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", id)));
    }

    @Override
    @Transactional
    public List<DomainUserDTO> querySourceOwner(final Long id) {
        ConnectionDO connection = getAnyConnectionByRequisitionId(id);
        Long sourceProjectId = connection.getSourceProject().getId();

        ProjectDO project = projectRepository.findById(sourceProjectId)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", sourceProjectId)));

        BizAssert.notFound(
                Objects.nonNull(project),
                "Not found source owner",
                String.format("Not found source owner projectId: %s", project.getId()));
        return Lists.newArrayList(new DomainUserDTO(project.getOwner()));
    }

    @Override
    @Transactional
    public DomainUserDTO getProposer(final Long id) {
        ConnectionDO connection = getAnyConnectionByRequisitionId(id);
        DomainUserDTO user = userRepository.findById(connection.getUser().getId())
                .map(DomainUserDTO::new)
                .orElseThrow(() -> new EntityNotFoundException(String.format("userId: %s", connection.getUser().getId())));

        return user;
    }

    @Override
    public void checkSourceOwnerPermissions(final Long id, final String domainAccount) {
        UserDTO user = userProcessService.getUserByDomainAccount(domainAccount);
        String email = user.getEmail();
        boolean isSourceOwner = getAnyConnectionByRequisitionId(id).getSourceProject().getOwner().getEmail().equals(email);
        BizAssert.notAuthorized(isSourceOwner, Authorization.INSUFFICIENT_PERMISSIONS);
    }

    @Override
    public void checkDbaPermissions(final Long id, final String domainAccount) {
        UserDTO user = userProcessService.getUserByDomainAccount(domainAccount);
        String email = user.getEmail();
        boolean isDba = userProcessService.isDba(email);
        BizAssert.notAuthorized(isDba, Authorization.INSUFFICIENT_PERMISSIONS);
    }

    @Override
    @Transactional
    public ConnectionRequisitionDTO save(final ConnectionRequisitionDetailDTO requisitionDetail) {
        ConnectionRequisitionDO connectionRequisitionDO = ConnectionRequisitionDetailDTO
                .toConnectionRequisitionDO(requisitionDetail);
        return ConnectionRequisitionDTO
                .toConnectionRequisitionDTO(connectionRequisitionRepository.save(connectionRequisitionDO));
    }

    private ConnectionDO getAnyConnectionByRequisitionId(final Long id) {
        List<Long> connectionIds = getConnectionIdsByRequisitionId(id);
        Long anyConnectionId = connectionIds.stream().findAny().get();

        return connectionRepository.findById(anyConnectionId)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", id)));
    }

    private List<Long> getConnectionIdsByRequisitionId(final Long requisitionId) {
        List<ConnectionRequisitionConnectionMappingDO> mappings = connectionRequisitionConnectionMappingRepository
                .query(
                        ConnectionRequisitionConnectionMappingQuery.builder().connectionRequisitionId(requisitionId).build());

        return mappings.stream().map(it -> it.getConnection().getId()).collect(Collectors.toList());
    }

    private void appendRequisitionLinkUrl(final ConnectionRequisitionDetailDTO requisitionDetail) {
        if (Strings.isNullOrEmpty(baseUrl)) {
            return;
        }

        String newBaseUrl = baseUrl.replace("{connectionRequisitionId}", String.valueOf(requisitionDetail.getId()));
        requisitionDetail.setBaseUrl(newBaseUrl);
    }

    /**
     * 审批单处理,根据项目拆分后的审批单,每一个审批单的处理使用独立的事务.
     *
     * @param requisitionDetail requisitionDetail
     */
    @Transactional
    @Override
    public void createRequisition(final ConnectionRequisitionDetailDTO requisitionDetail, final String domainAccount) {
        List<ConnectionDetailDTO> toSaveConnections = requisitionDetail.getConnections();

        // 1. save connections
        List<ConnectionDTO> savedConnections = connectionProcessService.bulkCreateConnection(toSaveConnections, domainAccount);

        // 2. save requisition
        ConnectionRequisitionDTO savedConnectionRequisition = save(requisitionDetail);

        // 3. save mappings
        saveRequisitionAndConnectionMapping(savedConnections, savedConnectionRequisition);

        // 4. send approve email

        Long id = savedConnectionRequisition.getId();
        ApprovalEvent event = approvalStateMachine.getApprovalEventGenerator().generateApprovalEvent(id, ApprovalOperation.PASS);
        ApprovalContext context = ApprovalContext.builder().id(id).build();
        approvalStateMachine.fire(event, context);
    }

    @Transactional
    @Override
    public void updateApproveState(final Long connectionRequisitionId, final ApprovalState state) {
        ConnectionRequisitionDO connectionRequisition = connectionRequisitionRepository.findById(connectionRequisitionId).get();
        connectionRequisition.setState(state);
        connectionRequisition.setUpdateTime(Instant.now());
        connectionRequisitionRepository.save(connectionRequisition);
    }

    @Transactional
    @Override
    public void updateApproveStateByDBA(
            final Long connectionRequisitionId,
            final ApprovalState state,
            final String approveResult,
            final String dbaDomainAccount
    ) {

        UserDTO user = userProcessService.getUserByDomainAccount(dbaDomainAccount);
        ConnectionRequisitionDO connectionRequisition = connectionRequisitionRepository.findById(connectionRequisitionId).get();
        connectionRequisition.setState(state);
        connectionRequisition.setDbaApproveResult(approveResult);
        connectionRequisition.setDbaApproverUser(user.toUserDO());
        connectionRequisition.setUpdateTime(Instant.now());
        connectionRequisitionRepository.save(connectionRequisition);
    }

    @Transactional
    @Override
    public void updateApproveStateBySourceOwner(
            final Long connectionRequisitionId,
            final ApprovalState state,
            final String approveResult,
            final String sourceOwnerDomainAccount
    ) {

        UserDTO user = userProcessService.getUserByDomainAccount(sourceOwnerDomainAccount);

        ConnectionRequisitionDO connectionRequisition = connectionRequisitionRepository.findById(connectionRequisitionId).get();
        connectionRequisition.setState(state);
        connectionRequisition.setSourceApproveResult(approveResult);
        connectionRequisition.setSourceApproverUser(user.toUserDO());
        connectionRequisition.setUpdateTime(Instant.now());
        connectionRequisitionRepository.save(connectionRequisition);
    }

    @Override
    public void bindThirdPartyId(final Long id, final String thirdPartyId) {
        ConnectionRequisitionDO connectionRequisition = connectionRequisitionRepository.findById(id).get();
        connectionRequisition.setThirdPartyId(thirdPartyId);
        connectionRequisitionRepository.save(connectionRequisition);
    }

    @Override
    public void invalidRequisition(final Long id) {
        Set<Long> connectionIds = getRequisitionDetail(id).getConnections().stream()
                .map(it -> it.getId())
                .collect(Collectors.toSet());
        connectionProcessService.bulkDeleteConnection(connectionIds);
    }
}
