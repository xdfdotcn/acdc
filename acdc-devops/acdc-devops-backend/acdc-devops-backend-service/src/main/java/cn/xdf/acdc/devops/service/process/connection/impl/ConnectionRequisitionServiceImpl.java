package cn.xdf.acdc.devops.service.process.connection.impl;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionRequisitionDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionRequisitionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.dto.approve.ApproveDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionRequisitionDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.query.ConnectionQuery;
import cn.xdf.acdc.devops.core.domain.query.ConnectionRequisitionQuery;
import cn.xdf.acdc.devops.repository.ConnectionRepository;
import cn.xdf.acdc.devops.repository.ConnectionRequisitionRepository;
import cn.xdf.acdc.devops.repository.ProjectRepository;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.error.exceptions.NotAuthorizedException;
import cn.xdf.acdc.devops.service.process.connection.ConnectionRequisitionService;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connection.approval.ApprovalContext;
import cn.xdf.acdc.devops.service.process.connection.approval.ApprovalStateMachine;
import cn.xdf.acdc.devops.service.process.connection.approval.error.ApprovalProcessStateMatchErrorException;
import cn.xdf.acdc.devops.service.process.connection.approval.event.ApprovalEvent;
import cn.xdf.acdc.devops.service.process.connection.approval.event.ApprovalOperation;
import cn.xdf.acdc.devops.service.process.user.UserService;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.Client;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.Connection;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.Project;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.persistence.EntityManager;
import javax.persistence.EntityNotFoundException;
import javax.persistence.PersistenceContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class ConnectionRequisitionServiceImpl implements ConnectionRequisitionService {

    @Autowired
    private ConnectionService connectionService;

    @Autowired
    private ConnectionRequisitionService connectionRequisitionService;

    @Autowired
    private ConnectionRequisitionRepository connectionRequisitionRepository;

    @Autowired
    private ConnectionRepository connectionRepository;

    @Autowired
    private ProjectRepository projectRepository;

    @Autowired
    private UserService userService;

    @Autowired
    private ApprovalStateMachine approvalStateMachine;

    @Autowired
    private I18nService i18n;

    @PersistenceContext
    private EntityManager entityManager;

    @Value("${acdc.approval.base-url:''}")
    private String baseUrl;

    @Override
    public List<ConnectionRequisitionDetailDTO> createRequisitionWithAutoSplit(final ConnectionRequisitionDetailDTO requisition, final String domainAccount) {
        if (Objects.isNull(requisition)
                || CollectionUtils.isEmpty(requisition.getConnections())
                || Strings.isNullOrEmpty(domainAccount)
        ) {
            throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER));
        }
        List<ConnectionRequisitionDetailDTO> requisitions = splitRequisition(requisition);

        // TODO 1. 每个 requisition 的处理都在不同的事务中, 临时解决bug:批量提交的时候,因为某个 requisition 处理失败,导致事务的全部回滚
        // TODO 2. 目前 requisition 是按照项目维度进行拆分的, 如果批量中存在失败的 requisition, 则重新发起请求重试,
        // TODO 3. 方法级别的事务,会覆盖类级别的事务,方法需要声明为public才会被spring代理
        // TODO 4. 临时解决事务代理不生效的问题,注入一个本类的service对象
        List<ConnectionRequisitionDetailDTO> savedRequisitions = new ArrayList<>();
        requisitions.forEach(it -> savedRequisitions.add(connectionRequisitionService.create(it, domainAccount)));
        return savedRequisitions;
    }

    private List<ConnectionRequisitionDetailDTO> splitRequisition(final ConnectionRequisitionDetailDTO requisitionDTO) {
        List<ConnectionDetailDTO> connectionDetailDTOs = requisitionDTO.getConnections();

        Map<Long, List<ConnectionDetailDTO>> groupedConnection = connectionDetailDTOs.stream()
                .collect(Collectors.groupingBy(ConnectionDetailDTO::getSourceProjectId));

        return groupedConnection.entrySet().stream()
                .map(it -> new ConnectionRequisitionDetailDTO(requisitionDTO.getDescription(), it.getValue()))
                .collect(Collectors.toList());
    }

    @Override
    @Transactional
    public List<ConnectionRequisitionDTO> query(final ConnectionRequisitionQuery query) {
        return connectionRequisitionRepository.query(query)
                .stream().map(ConnectionRequisitionDTO::new)
                .collect(Collectors.toList());
    }

    @Override
    @Transactional
    public void approveRequisitionConnections(final Long id) {
        List<Long> connectionIds = getDetailById(id).getConnections().stream().map(it -> it.getId()).collect(Collectors.toList());

        ConnectionQuery connectionQuery = ConnectionQuery.builder()
                .requisitionState(RequisitionState.APPROVING)
                .connectionIds(connectionIds)
                .build();

        connectionService.updateConnectionRequisitionStateByQuery(connectionQuery, RequisitionState.APPROVED);
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
    @Transactional
    public ConnectionRequisitionDTO getByThirdPartyId(final String thirdPartyId) {
        return connectionRequisitionRepository.findByThirdPartyId(thirdPartyId)
                .map(ConnectionRequisitionDTO::new)
                .orElseThrow(() -> new ApprovalProcessStateMatchErrorException(String.format("Not found entity, thirdPartyId: %s", thirdPartyId)));
    }

    @Override
    @Transactional
    public ConnectionRequisitionDetailDTO getDetailById(final Long id) {
        ConnectionRequisitionDO requisitionDO = connectionRequisitionRepository.findById(id)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(Connection.CONNECTION_REQUISITION_NOT_FOUND, id)));
        ConnectionRequisitionDetailDTO requisitionDetail = new ConnectionRequisitionDetailDTO(
                requisitionDO
        );

        appendRequisitionLinkUrl(requisitionDetail);

        return requisitionDetail;
    }

    @Override
    @Transactional
    public ConnectionRequisitionDTO getById(final Long id) {
        return connectionRequisitionRepository.findById(id)
                .map(ConnectionRequisitionDTO::new)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(Connection.CONNECTION_REQUISITION_NOT_FOUND, id)));
    }

    @Override
    @Transactional
    public List<UserDTO> getSourceOwners(final Long id) {
        ConnectionDetailDTO connection = getDetailById(id).getConnections().get(0);
        Long sourceProjectId = connection.getSourceProjectId();

        UserDO projectOwnerDO = projectRepository.findById(sourceProjectId)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(Project.NOT_FOUND, sourceProjectId)))
                .getOwner();

        return Lists.newArrayList(new UserDTO(projectOwnerDO));
    }

    @Override
    @Transactional
    public UserDTO getProposer(final Long id) {
        ConnectionDO connection = getAnyConnectionByRequisitionId(id);
        return new UserDTO(connection.getUser());
    }

    @Override
    @Transactional
    public void checkSourceOwnerPermissions(final Long id, final String domainAccount) {
        UserDTO user = userService.getByDomainAccount(domainAccount);
        String email = user.getEmail();
        boolean isSourceOwner = getAnyConnectionByRequisitionId(id).getSourceProject().getOwner().getEmail().equals(email);

        if (!isSourceOwner) {
            throw new NotAuthorizedException(i18n.msg(I18nKey.Authorization.INSUFFICIENT_PERMISSIONS));
        }
    }

    @Override
    @Transactional
    public void checkDbaPermissions(final String domainAccount) {
        UserDTO user = userService.getByDomainAccount(domainAccount);
        boolean isDba = userService.isDBA(user.getDomainAccount());
        if (!isDba) {
            throw new NotAuthorizedException(i18n.msg(I18nKey.Authorization.INSUFFICIENT_PERMISSIONS));
        }
    }

    private ConnectionDO getAnyConnectionByRequisitionId(final Long id) {
        Long anyConnectionId = getDetailById(id).getConnections().get(0).getId();

        return connectionRepository.findById(anyConnectionId)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(Connection.NOT_FOUND, anyConnectionId)));
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
    public ConnectionRequisitionDetailDTO create(final ConnectionRequisitionDetailDTO requisitionDetail, final String domainAccount) {
        List<ConnectionDetailDTO> toSaveConnections = requisitionDetail.getConnections();

        // 1. save connections
        List<ConnectionDetailDTO> savedConnections = connectionService.batchCreate(toSaveConnections, domainAccount);
        requisitionDetail.setConnections(savedConnections);

        // 2. save requisition, relation of requisition, connections
        ConnectionRequisitionDetailDTO savedConnectionRequisition = saveRequisition(requisitionDetail);

        // 3. send approve email
        Long id = savedConnectionRequisition.getId();
        ApprovalEvent event = approvalStateMachine.getApprovalEventGenerator().generateApprovalEvent(id, ApprovalOperation.PASS);
        ApprovalContext context = ApprovalContext.builder().id(id).operatorId(domainAccount).build();
        approvalStateMachine.fire(event, context);
        return savedConnectionRequisition;
    }

    @Transactional
    protected ConnectionRequisitionDetailDTO saveRequisition(final ConnectionRequisitionDetailDTO requisitionDetail) {
        ConnectionRequisitionDO savedRequisition = connectionRequisitionRepository.save(requisitionDetail.toDO());
        entityManager.refresh(savedRequisition);
        return new ConnectionRequisitionDetailDTO(savedRequisition);
    }

    @Transactional
    @Override
    public void updateApproveState(final Long connectionRequisitionId, final ApprovalState state) {
        ConnectionRequisitionDO connectionRequisition = connectionRequisitionRepository.findById(connectionRequisitionId).get();
        connectionRequisition.setState(state);
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

        UserDTO user = userService.getByDomainAccount(dbaDomainAccount);
        ConnectionRequisitionDO connectionRequisition = connectionRequisitionRepository.findById(connectionRequisitionId).get();
        connectionRequisition.setState(state);
        connectionRequisition.setDbaApproveResult(approveResult);
        connectionRequisition.setDbaApproverUser(user.toDO());
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
        UserDTO user = userService.getByDomainAccount(sourceOwnerDomainAccount);

        ConnectionRequisitionDO connectionRequisition = connectionRequisitionRepository.findById(connectionRequisitionId).get();
        connectionRequisition.setState(state);
        connectionRequisition.setSourceApproveResult(approveResult);
        connectionRequisition.setSourceApproverUser(user.toDO());
        connectionRequisitionRepository.save(connectionRequisition);
    }

    @Override
    @Transactional
    public void bindThirdPartyId(final Long id, final String thirdPartyId) {
        ConnectionRequisitionDO connectionRequisition = connectionRequisitionRepository.findById(id).get();
        connectionRequisition.setThirdPartyId(thirdPartyId);
        connectionRequisitionRepository.save(connectionRequisition);
    }

    @Override
    @Transactional
    public void invalidRequisition(final Long id) {
        Set<Long> connectionIds = getDetailById(id).getConnections().stream()
                .map(it -> it.getId())
                .collect(Collectors.toSet());
        connectionService.deleteByIds(connectionIds);
    }
}
