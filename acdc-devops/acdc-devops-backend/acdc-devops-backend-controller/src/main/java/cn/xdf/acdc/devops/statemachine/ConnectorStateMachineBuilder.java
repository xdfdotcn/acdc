package cn.xdf.acdc.devops.statemachine;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorEvent;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import cn.xdf.acdc.devops.statemachine.actions.CreatingConnectorAction;
import cn.xdf.acdc.devops.statemachine.actions.RestartingConnectorAction;
import cn.xdf.acdc.devops.statemachine.actions.StoppingConnectorAction;
import cn.xdf.acdc.devops.statemachine.actions.UpdateStateToDbAction;
import cn.xdf.acdc.devops.statemachine.actions.UpdatingConnectorAction;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.squirrelframework.foundation.fsm.Action;
import org.squirrelframework.foundation.fsm.StateMachineBuilder;
import org.squirrelframework.foundation.fsm.StateMachineBuilderFactory;

import java.util.Arrays;

@Configuration
public class ConnectorStateMachineBuilder implements ApplicationContextAware {

    private ApplicationContext applicationContext;

    private final UpdateStateToDbAction updateStateToDbAction;

    private final CreatingConnectorAction creatingConnectorAction;

    private final RestartingConnectorAction restartingConnectorAction;

    private final UpdatingConnectorAction updatingConnectorAction;

    private final StoppingConnectorAction stoppingConnectorAction;

    /**
     * Construct a instance with all args.
     *
     * @param updateStateToDbAction updateStateToDbAction
     * @param creatingConnectorAction creatingConnectorAction
     * @param restartingConnectorAction restartingConnectorAction
     * @param updatingConnectorAction updatingConnectorAction
     * @param stoppingConnectorAction stoppingConnectorAction
     */
    public ConnectorStateMachineBuilder(final UpdateStateToDbAction updateStateToDbAction, final CreatingConnectorAction creatingConnectorAction,
                                        final RestartingConnectorAction restartingConnectorAction, final UpdatingConnectorAction updatingConnectorAction,
                                        final StoppingConnectorAction stoppingConnectorAction) {
        this.updateStateToDbAction = updateStateToDbAction;
        this.creatingConnectorAction = creatingConnectorAction;
        this.restartingConnectorAction = restartingConnectorAction;
        this.updatingConnectorAction = updatingConnectorAction;
        this.stoppingConnectorAction = stoppingConnectorAction;
    }

    /**
     * Define a state machine builder according to real state transition.
     * refer to: https://github.com/hekailiang/squirrel
     *
     * @return state machine builder
     */
    @Bean
    public StateMachineBuilder<ConnectorStateMachine, ConnectorState, ConnectorEvent, ConnectorInfoDTO> getStateMachineBuilder() {
        StateMachineBuilder<ConnectorStateMachine, ConnectorState, ConnectorEvent, ConnectorInfoDTO> builder = StateMachineBuilderFactory
                .create(ConnectorStateMachine.class, ConnectorState.class, ConnectorEvent.class, ConnectorInfoDTO.class);

        Arrays.stream(ConnectorStateTransitionTable.values()).forEach(table ->
            builder.externalTransition().from(table.getFrom()).to(table.getTo()).on(table.getEvent()).perform(getActionByClass(table.getActionClass())));

        return builder;
    }

    private Action<ConnectorStateMachine, ConnectorState, ConnectorEvent, ConnectorInfoDTO> getActionByClass(final Class actionClass) {
        String className = actionClass.getSimpleName();
        String beanName = classNameToBeanName(className);
        return (Action<ConnectorStateMachine, ConnectorState, ConnectorEvent, ConnectorInfoDTO>) applicationContext.getBean(beanName);
    }

    private String classNameToBeanName(final String className) {
        String firstChar = className.substring(0, 1).toLowerCase();
        return firstChar + className.substring(1);
    }

    private <T> boolean checkIsInstanceOf(final T action, final Class actionClass) {
        return action.getClass().getName().startsWith(actionClass.getName());
    }

    @Override
    public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
