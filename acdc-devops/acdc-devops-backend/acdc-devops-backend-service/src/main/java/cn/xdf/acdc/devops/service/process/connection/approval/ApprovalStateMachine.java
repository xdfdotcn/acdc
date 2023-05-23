package cn.xdf.acdc.devops.service.process.connection.approval;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionRequisitionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.AuthorityRoleType;
import cn.xdf.acdc.devops.core.domain.query.UserAuthorityQuery;
import cn.xdf.acdc.devops.core.domain.query.UserQuery;
import cn.xdf.acdc.devops.repository.ConnectionRepository;
import cn.xdf.acdc.devops.repository.UserAuthorityRepository;
import cn.xdf.acdc.devops.repository.UserRepository;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.process.connection.ConnectionRequisitionService;
import cn.xdf.acdc.devops.service.process.connection.approval.action.ApprovalAction;
import cn.xdf.acdc.devops.service.process.connection.approval.definition.DefaultApprovalStateMachineDefinitionFactory;
import cn.xdf.acdc.devops.service.process.connection.approval.error.ApprovalProcessStateMatchErrorException;
import cn.xdf.acdc.devops.service.process.connection.approval.event.ApprovalEvent;
import cn.xdf.acdc.devops.service.process.connection.approval.event.ApprovalEventGenerator;
import cn.xdf.acdc.devops.service.utility.mail.DomainUser;
import com.google.common.collect.Sets;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Approve machine.
 */

@Component
public class ApprovalStateMachine implements ApplicationContextAware {
    
    private final Map<ActionMapping.ActionMappingKey, ActionMapping> stateMachineDefinitionConfig;
    
    private final Map<ApprovalState, Set<ApprovalEvent>> stateEventConfig;
    
    @Autowired
    private ConnectionRequisitionService connectionRequisitionService;
    
    @Autowired
    private UserAuthorityRepository userAuthorityRepository;
    
    @Autowired
    private UserRepository userRepository;
    
    @Autowired
    private ConnectionRepository connectionRepository;
    
    @Autowired
    private ApprovalEventGenerator approvalEventGenerator;
    
    private ApplicationContext applicationContext;
    
    public ApprovalStateMachine() {
        stateMachineDefinitionConfig = new DefaultApprovalStateMachineDefinitionFactory()
                .createApprovalStateMachineDefinition()
                .getStateMachineDefinition();
        
        stateEventConfig = stateMachineDefinitionConfig.keySet().stream()
                .collect(Collectors.groupingBy(it -> it.getFrom(),
                        Collectors.mapping(it -> it.getEvent(), Collectors.toSet()))
                );
    }
    
    /**
     * 状态机执行事件.
     *
     * @param event event
     * @param approvalContext approvalContext
     */
    @Transactional
    public void fire(final ApprovalEvent event, final ApprovalContext approvalContext) {
        Long id = approvalContext.getId();
        ApprovalState currentState = currentState(id);
        
        ActionMapping actionMapping = Optional.ofNullable(stateMachineDefinitionConfig.get(ActionMapping.ActionMappingKey.of(currentState, event)))
                .orElseThrow(() -> new ApprovalProcessStateMatchErrorException(
                        String.format("Error while approving, unknown event : %s, current state: %s, id: %s", event, currentState, id)
                ));
        getActionInstance(actionMapping.getActionClass()).action(actionMapping.getFrom(), actionMapping.getTo(), event, approvalContext, this);
    }
    
    @Override
    public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
    
    /**
     * 获取Action 实例.
     *
     * @param actionClass actionClass
     * @return ApprovalAction
     */
    public ApprovalAction getActionInstance(final Class<? extends ApprovalAction> actionClass) {
        return applicationContext.getBean(actionClass);
    }
    
    
    /**
     * 获取当前审批状态.
     *
     * @param id id
     * @return ApprovalState
     */
    @Transactional
    public ApprovalState currentState(final Long id) {
        return connectionRequisitionService.getById(id).getState();
    }
    
    /**
     * 获取申请单详情.
     *
     * @param id id
     * @return ConnectionRequisitionDetailDTO
     */
    @Transactional
    public ConnectionRequisitionDetailDTO getConnectionRequisitionById(final Long id) {
        return connectionRequisitionService.getDetailById(id);
    }
    
    /**
     * 获取数据源审批人.
     *
     * @param id id
     * @return List
     */
    @Transactional
    public List<DomainUser> getSourceOwners(final Long id) {
        return connectionRequisitionService.getSourceOwners(id)
                .stream().map(it -> new DomainUser(it.getEmail(), it.getName()))
                .collect(Collectors.toList());
    }
    
    /**
     * 获取DBA审批人.
     *
     * @param id id
     * @return List
     */
    @Transactional
    public List<DomainUser> getDbaApprovalUser(final Long id) {
        UserAuthorityQuery userAuthorityQuery = new UserAuthorityQuery()
                .setAuthorityRoleTypes(Sets.newHashSet(AuthorityRoleType.ROLE_DBA));
        
        Set<Long> userIds = userAuthorityRepository.queryAll(userAuthorityQuery)
                .stream().map(it -> it.getUserId()).collect(Collectors.toSet());
        
        if (CollectionUtils.isEmpty(userIds)) {
            throw new ClientErrorException("The DBA approve users must not be empty");
        }
        
        UserQuery userQuery = new UserQuery()
                .setUserIds(userIds);
        
        return userRepository.query(userQuery).stream()
                .map(it -> new DomainUser(it.getEmail(), it.getName()))
                .collect(Collectors.toList());
    }
    
    /**
     * 获取申请人.
     *
     * @param id id
     * @return DomainUserDTO
     */
    @Transactional
    public DomainUser getProposer(final Long id) {
        UserDTO userDTO = connectionRequisitionService.getProposer(id);
        return new DomainUser(userDTO.getEmail(), userDTO.getName());
    }
    
    /**
     * 获取申请的链路列表.
     *
     * @param id id
     * @return List
     */
    @Transactional
    public List<ConnectionDetailDTO> getConnections(final Long id) {
        return connectionRequisitionService.getDetailById(id).getConnections();
    }
    
    /**
     * 审批事件校验.
     *
     * @param id id
     * @param event event
     */
    public void verifyEvent(final Long id, final ApprovalEvent event) {
        ApprovalState currentState = currentState(id);
        Set<ApprovalEvent> supportEvents = stateEventConfig.get(currentState);
        if (CollectionUtils.isEmpty(supportEvents) || !supportEvents.contains(event)) {
            throw new ApprovalProcessStateMatchErrorException(
                    String.format("Approval state machine verify event failure"
                            + ",  event : %s, current state: %s, id: %s", event, currentState, id)
            );
        }
    }
    
    /**
     * 获取事件生成器.
     *
     * @return approvalEventGenerator
     */
    public ApprovalEventGenerator getApprovalEventGenerator() {
        return approvalEventGenerator;
    }
}
