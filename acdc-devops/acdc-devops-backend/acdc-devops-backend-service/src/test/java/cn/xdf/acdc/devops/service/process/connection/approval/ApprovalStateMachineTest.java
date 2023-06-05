package cn.xdf.acdc.devops.service.process.connection.approval;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionRequisitionDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.service.process.connection.ConnectionRequisitionService;
import cn.xdf.acdc.devops.service.process.connection.approval.ActionMapping.ActionMappingKey;
import cn.xdf.acdc.devops.service.process.connection.approval.action.ApprovalAction;
import cn.xdf.acdc.devops.service.process.connection.approval.action.DbaApprovedAction;
import cn.xdf.acdc.devops.service.process.connection.approval.action.DbaRefusedAction;
import cn.xdf.acdc.devops.service.process.connection.approval.action.SendApprovalAction;
import cn.xdf.acdc.devops.service.process.connection.approval.action.SkipAllApprovalAction;
import cn.xdf.acdc.devops.service.process.connection.approval.action.SourceOwnerApprovedAction;
import cn.xdf.acdc.devops.service.process.connection.approval.action.SourceOwnerApprovedAndSkipDbaApprovalAction;
import cn.xdf.acdc.devops.service.process.connection.approval.action.SourceOwnerRefusedAction;
import cn.xdf.acdc.devops.service.process.connection.approval.definition.DefaultApprovalStateMachineDefinitionFactory;
import cn.xdf.acdc.devops.service.process.connection.approval.error.ApprovalProcessStateMatchErrorException;
import cn.xdf.acdc.devops.service.process.connection.approval.event.ApprovalEvent;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ApprovalStateMachineTest {
    
    @Mock
    private SendApprovalAction sendApprovalAction;
    
    @Mock
    private SourceOwnerApprovedAction sourceOwnerApprovedAction;
    
    @Mock
    private SourceOwnerRefusedAction sourceOwnerRefusedAction;
    
    @Mock
    private DbaApprovedAction dbaApprovedAction;
    
    @Mock
    private DbaRefusedAction dbaRefusedAction;
    
    @Mock
    private SkipAllApprovalAction skipAllApprovalAction;
    
    @Mock
    private ApplicationContext applicationContext;
    
    @Mock
    private SourceOwnerApprovedAndSkipDbaApprovalAction sourceOwnerApprovedAndSkipDbaApprovalAction;
    
    @Mock
    private ConnectionRequisitionService connectionRequisitionService;
    
    private Map<String, ApprovalAction> actionMap = new HashMap<>();
    
    @Before
    public void setup() {
        when(applicationContext.getBean(SendApprovalAction.class)).thenReturn(sendApprovalAction);
        when(applicationContext.getBean(SourceOwnerApprovedAction.class)).thenReturn(sourceOwnerApprovedAction);
        when(applicationContext.getBean(SourceOwnerRefusedAction.class)).thenReturn(sourceOwnerRefusedAction);
        when(applicationContext.getBean(DbaApprovedAction.class)).thenReturn(dbaApprovedAction);
        when(applicationContext.getBean(DbaRefusedAction.class)).thenReturn(dbaRefusedAction);
        when(applicationContext.getBean(SkipAllApprovalAction.class)).thenReturn(skipAllApprovalAction);
        when(applicationContext.getBean(SourceOwnerApprovedAndSkipDbaApprovalAction.class)).thenReturn(sourceOwnerApprovedAndSkipDbaApprovalAction);
    }
    
    @Test
    public void testApprovingToSourceOwnerApproving() {
        ApprovalStateMachine machine = createStateMachine();
        
        when(connectionRequisitionService.getById(any()))
                .thenReturn(new ConnectionRequisitionDTO().setState(ApprovalState.APPROVING));
        
        machine.fire(ApprovalEvent.PASS, new ApprovalContext());
        
        Mockito.verify(sendApprovalAction, Mockito.times(1))
                .action(
                        eq(ApprovalState.APPROVING),
                        eq(ApprovalState.SOURCE_OWNER_APPROVING),
                        any(), any(), any());
        verifyOtherActionInvokeTimes(sendApprovalAction);
    }
    
    @Test
    public void testApprovingToSourceOwnerApproved() {
        ApprovalStateMachine machine = createStateMachine();
        
        when(connectionRequisitionService.getById(any()))
                .thenReturn(new ConnectionRequisitionDTO().setState(ApprovalState.APPROVING));
        
        machine.fire(ApprovalEvent.PASS_AND_SKIP_REMAIN, new ApprovalContext());
        
        Mockito.verify(skipAllApprovalAction, Mockito.times(1))
                .action(
                        eq(ApprovalState.APPROVING),
                        eq(ApprovalState.APPROVED),
                        any(), any(), any());
        verifyOtherActionInvokeTimes(skipAllApprovalAction);
    }
    
    @Test
    public void testSourceOwnerApprovingToDbaApproving() {
        ApprovalStateMachine machine = createStateMachine();
        when(connectionRequisitionService.getById(any()))
                .thenReturn(new ConnectionRequisitionDTO().setState(ApprovalState.SOURCE_OWNER_APPROVING));
        
        machine.fire(ApprovalEvent.PASS, new ApprovalContext());
        
        Mockito.verify(sourceOwnerApprovedAction, Mockito.times(1))
                .action(
                        eq(ApprovalState.SOURCE_OWNER_APPROVING),
                        eq(ApprovalState.DBA_APPROVING),
                        any(), any(), any());
        verifyOtherActionInvokeTimes(sourceOwnerApprovedAction);
    }
    
    //
    @Test
    public void testSourceOwnerApprovingToApproved() {
        ApprovalStateMachine machine = createStateMachine();
        when(connectionRequisitionService.getById(any()))
                .thenReturn(new ConnectionRequisitionDTO().setState(ApprovalState.SOURCE_OWNER_APPROVING));
        
        machine.fire(ApprovalEvent.PASS_AND_SKIP_REMAIN, new ApprovalContext());
        
        Mockito.verify(sourceOwnerApprovedAndSkipDbaApprovalAction, Mockito.times(1))
                .action(
                        eq(ApprovalState.SOURCE_OWNER_APPROVING),
                        eq(ApprovalState.APPROVED),
                        any(), any(), any());
        verifyOtherActionInvokeTimes(sourceOwnerApprovedAndSkipDbaApprovalAction);
    }
    
    //
    @Test
    public void testSourceOwnerApprovingToSourceOwnerRefused() {
        ApprovalStateMachine machine = createStateMachine();
        when(connectionRequisitionService.getById(any()))
                .thenReturn(new ConnectionRequisitionDTO().setState(ApprovalState.SOURCE_OWNER_APPROVING));
        
        machine.fire(ApprovalEvent.REFUSED, new ApprovalContext());
        
        Mockito.verify(sourceOwnerRefusedAction, Mockito.times(1))
                .action(
                        eq(ApprovalState.SOURCE_OWNER_APPROVING),
                        eq(ApprovalState.SOURCE_OWNER_REFUSED),
                        any(), any(), any());
        verifyOtherActionInvokeTimes(sourceOwnerRefusedAction);
    }
    
    //
    @Test
    public void testSourceOwnerRefusedToDbaApproving() {
        ApprovalStateMachine machine = createStateMachine();
        
        when(connectionRequisitionService.getById(any()))
                .thenReturn(new ConnectionRequisitionDTO().setState(ApprovalState.SOURCE_OWNER_REFUSED));
        
        machine.fire(ApprovalEvent.PASS, new ApprovalContext());
        
        Mockito.verify(sourceOwnerApprovedAction, Mockito.times(1))
                .action(
                        eq(ApprovalState.SOURCE_OWNER_REFUSED),
                        eq(ApprovalState.DBA_APPROVING),
                        any(), any(), any());
        verifyOtherActionInvokeTimes(sourceOwnerApprovedAction);
    }
    
    //
    @Test
    public void testSourceOwnerRefusedToApproved() {
        ApprovalStateMachine machine = createStateMachine();
        when(connectionRequisitionService.getById(any()))
                .thenReturn(new ConnectionRequisitionDTO().setState(ApprovalState.SOURCE_OWNER_REFUSED));
        
        machine.fire(ApprovalEvent.PASS_AND_SKIP_REMAIN, new ApprovalContext());
        
        Mockito.verify(sourceOwnerApprovedAndSkipDbaApprovalAction, Mockito.times(1))
                .action(
                        eq(ApprovalState.SOURCE_OWNER_REFUSED),
                        eq(ApprovalState.APPROVED),
                        any(), any(), any());
        verifyOtherActionInvokeTimes(sourceOwnerApprovedAndSkipDbaApprovalAction);
    }
    
    //
    @Test
    public void testDbaApprovingToApproved() {
        ApprovalStateMachine machine = createStateMachine();
        when(connectionRequisitionService.getById(any()))
                .thenReturn(new ConnectionRequisitionDTO().setState(ApprovalState.DBA_APPROVING));
        
        machine.fire(ApprovalEvent.PASS, new ApprovalContext());
        
        Mockito.verify(dbaApprovedAction, Mockito.times(1))
                .action(
                        eq(ApprovalState.DBA_APPROVING),
                        eq(ApprovalState.APPROVED),
                        any(), any(), any());
        verifyOtherActionInvokeTimes(dbaApprovedAction);
    }
    
    //
    @Test
    public void testDbaApprovingToDbaRefused() {
        ApprovalStateMachine machine = createStateMachine();
        when(connectionRequisitionService.getById(any()))
                .thenReturn(new ConnectionRequisitionDTO().setState(ApprovalState.DBA_APPROVING));
        
        machine.fire(ApprovalEvent.REFUSED, new ApprovalContext());
        
        Mockito.verify(dbaRefusedAction, Mockito.times(1))
                .action(
                        eq(ApprovalState.DBA_APPROVING),
                        eq(ApprovalState.DBA_REFUSED),
                        any(), any(), any());
        verifyOtherActionInvokeTimes(dbaRefusedAction);
    }
    
    //
    @Test
    public void testDbaRefusedToApproved() {
        ApprovalStateMachine machine = createStateMachine();
        when(connectionRequisitionService.getById(any()))
                .thenReturn(new ConnectionRequisitionDTO().setState(ApprovalState.DBA_REFUSED));
        
        machine.fire(ApprovalEvent.PASS, new ApprovalContext());
        
        Mockito.verify(dbaApprovedAction, Mockito.times(1))
                .action(
                        eq(ApprovalState.DBA_REFUSED),
                        eq(ApprovalState.APPROVED),
                        any(), any(), any());
        
        verifyOtherActionInvokeTimes(dbaApprovedAction);
    }
    
    //
    @Test
    public void testTransformShouldThrowExceptionWhenNotMatchEvent() {
        ApprovalStateMachine machine = createStateMachine();
        // APPROVING
        verifyTransform(
                machine,
                ApprovalState.APPROVING,
                Arrays.stream(ApprovalEvent.values())
                        .filter(it -> it != ApprovalEvent.PASS)
                        .filter(it -> it != ApprovalEvent.PASS_AND_SKIP_REMAIN)
                        .collect(Collectors.toList())
        );
        
        // SOURCE_OWNER_APPROVING
        verifyTransform(
                machine,
                ApprovalState.SOURCE_OWNER_APPROVING,
                Arrays.stream(ApprovalEvent.values())
                        .filter(it -> it != ApprovalEvent.PASS)
                        .filter(it -> it != ApprovalEvent.REFUSED)
                        .filter(it -> it != ApprovalEvent.PASS_AND_SKIP_REMAIN)
                        .collect(Collectors.toList())
        );
        
        // SOURCE_OWNER_REFUSED
        verifyTransform(
                machine,
                ApprovalState.SOURCE_OWNER_REFUSED,
                Arrays.stream(ApprovalEvent.values())
                        .filter(it -> it != ApprovalEvent.PASS)
                        .filter(it -> it != ApprovalEvent.PASS_AND_SKIP_REMAIN)
                        .collect(Collectors.toList())
        );
        
        // DBA_APPROVING
        verifyTransform(
                machine,
                ApprovalState.DBA_APPROVING,
                Arrays.stream(ApprovalEvent.values())
                        .filter(it -> it != ApprovalEvent.PASS)
                        .filter(it -> it != ApprovalEvent.REFUSED)
                        .collect(Collectors.toList())
        );
        
        // DBA_REFUSED
        verifyTransform(
                machine,
                ApprovalState.DBA_REFUSED,
                Arrays.stream(ApprovalEvent.values())
                        .filter(it -> it != ApprovalEvent.PASS)
                        .collect(Collectors.toList())
        );
        
        // APPROVED, ps: 已通过的审批,不能重新触发审批状态扭转
        verifyTransform(
                machine,
                ApprovalState.APPROVED,
                Arrays.stream(ApprovalEvent.values())
                        .collect(Collectors.toList())
        );
    }
    
    //
    @Test
    public void testActionMapping() {
        Map<ActionMapping.ActionMappingKey, ActionMapping> stateDefinition = new DefaultApprovalStateMachineDefinitionFactory()
                .createApprovalStateMachineDefinition()
                .getStateMachineDefinition();
        
        ActionMapping actionMapping = stateDefinition.get(ActionMappingKey.of(ApprovalState.APPROVING, ApprovalEvent.PASS));
        Assertions.assertThat(actionMapping.getActionClass())
                .isEqualTo(SendApprovalAction.class);
        
        actionMapping = stateDefinition.get(ActionMappingKey.of(ApprovalState.APPROVING, ApprovalEvent.PASS_AND_SKIP_REMAIN));
        Assertions.assertThat(actionMapping.getActionClass())
                .isEqualTo(SkipAllApprovalAction.class);
        
        actionMapping = stateDefinition
                .get(ActionMappingKey.of(ApprovalState.SOURCE_OWNER_APPROVING, ApprovalEvent.PASS));
        Assertions.assertThat(actionMapping.getActionClass())
                .isEqualTo(SourceOwnerApprovedAction.class);
        
        actionMapping = stateDefinition
                .get(ActionMappingKey.of(ApprovalState.SOURCE_OWNER_APPROVING, ApprovalEvent.PASS_AND_SKIP_REMAIN));
        Assertions.assertThat(actionMapping.getActionClass())
                .isEqualTo(SourceOwnerApprovedAndSkipDbaApprovalAction.class);
        
        actionMapping = stateDefinition.get(ActionMappingKey.of(ApprovalState.SOURCE_OWNER_APPROVING, ApprovalEvent.REFUSED));
        Assertions.assertThat(actionMapping.getActionClass())
                .isEqualTo(SourceOwnerRefusedAction.class);
        
        actionMapping = stateDefinition.get(ActionMappingKey.of(ApprovalState.SOURCE_OWNER_REFUSED, ApprovalEvent.PASS));
        Assertions.assertThat(actionMapping.getActionClass())
                .isEqualTo(SourceOwnerApprovedAction.class);
        
        actionMapping = stateDefinition
                .get(ActionMappingKey.of(ApprovalState.SOURCE_OWNER_REFUSED, ApprovalEvent.PASS_AND_SKIP_REMAIN));
        Assertions.assertThat(actionMapping.getActionClass())
                .isEqualTo(SourceOwnerApprovedAndSkipDbaApprovalAction.class);
        
        actionMapping = stateDefinition.get(ActionMappingKey.of(ApprovalState.DBA_APPROVING, ApprovalEvent.PASS));
        Assertions.assertThat(actionMapping.getActionClass())
                .isEqualTo(DbaApprovedAction.class);
        
        actionMapping = stateDefinition.get(ActionMappingKey.of(ApprovalState.DBA_APPROVING, ApprovalEvent.REFUSED));
        Assertions.assertThat(actionMapping.getActionClass())
                .isEqualTo(DbaRefusedAction.class);
        
        actionMapping = stateDefinition.get(ActionMappingKey.of(ApprovalState.DBA_REFUSED, ApprovalEvent.PASS));
        Assertions.assertThat(actionMapping.getActionClass())
                .isEqualTo(DbaApprovedAction.class);
    }
    
    private void verifyOtherActionInvokeTimes(
            final ApprovalAction currentAction
    ) {
        
        List<ApprovalAction> actions = actionMap.values().stream()
                .filter(it -> !it.getClass().getName().equals(currentAction.getClass().getName()))
                .collect(Collectors.toList());
        
        actions.forEach(it -> Mockito.verify(it, Mockito.times(0)).action(any(), any(), any(), any(), any()));
    }
    
    private void verifyTransform(
            final ApprovalStateMachine machine,
            final ApprovalState currentState,
            final List<ApprovalEvent> otherEvents
    ) {
        
        when(connectionRequisitionService.getById(any()))
                .thenReturn(new ConnectionRequisitionDTO().setState(currentState));
        
        otherEvents.forEach(it -> {
            Throwable throwable = Assertions.catchThrowable(() -> machine.fire(it, new ApprovalContext()));
            Assertions.assertThat(throwable).isInstanceOf(ApprovalProcessStateMatchErrorException.class);
        });
    }
    
    private static void setFinalStaticField(final Class<?> clazz, final String fieldName, final Object value)
            throws ReflectiveOperationException {
        
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        
        Field modifiers = Field.class.getDeclaredField("modifiers");
        modifiers.setAccessible(true);
        field.set(null, value);
    }
    
    private ApprovalStateMachine createStateMachine() {
        ApprovalStateMachine machine = new ApprovalStateMachine();
        ReflectionTestUtils.setField(machine, "applicationContext", applicationContext);
        ReflectionTestUtils.setField(machine, "connectionRequisitionService", connectionRequisitionService);
        return machine;
    }
}
