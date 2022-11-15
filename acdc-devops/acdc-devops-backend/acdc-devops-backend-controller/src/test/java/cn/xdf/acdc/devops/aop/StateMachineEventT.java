package cn.xdf.acdc.devops.aop;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorEventDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.EventLevel;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.EventReason;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.EventSource;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import cn.xdf.acdc.devops.service.aop.Event;
import cn.xdf.acdc.devops.service.entity.ConnectorEventService;
import cn.xdf.acdc.devops.statemachine.actions.UpdateStateToDbAction;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.FilterType;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {StateMachineEventT.AopConfig.class})
public class StateMachineEventT {

    @Autowired
    private EventAspectAopTest eventAspectAopTest;

    @MockBean
    private ConnectorEventService connectorEventService;

    @Test
    public void testEventAnnotationWithProperties() {
        ConnectorInfoDTO connectorInfoDTO = new ConnectorInfoDTO();
        connectorInfoDTO.setId(1L);
        connectorInfoDTO.setRemark("exception xxx");
        eventAspectAopTest.testStateMachineEventAdapterAndEventAnnotationWithProperties(ConnectorState.PENDING, ConnectorState.CREATION_FAILED,
                cn.xdf.acdc.devops.core.domain.enumeration.ConnectorEvent.CREATE_FAILURE, connectorInfoDTO);
        ArgumentCaptor<ConnectorEventDO> eventCaptor = ArgumentCaptor.forClass(ConnectorEventDO.class);
        Mockito.verify(connectorEventService).save(eventCaptor.capture());
        ConnectorEventDO event = eventCaptor.getValue();
        Assertions.assertThat(event.getLevel()).isEqualTo(EventLevel.ERROR);
        Assertions.assertThat(event.getReason()).isEqualTo(EventReason.CONNECTOR_ACTUAL_STATUS_CHANGED.getName());
        Assertions.assertThat(event.getConnector().getId()).isEqualTo(1L);
        Assertions.assertThat(event.getSource()).isEqualTo(EventSource.ACDC_SCHEDULER);
        Assertions.assertThat(event.getMessage()).isEqualTo("start failed, error info: \nexception xxx");
    }

    @Component
    static class EventAspectAopTest {

        @Event(connectorId = "#connectorInfoDTO.id", reason = EventReason.CONNECTOR_ACTUAL_STATUS_CHANGED, source = EventSource.ACDC_SCHEDULER,
                level = UpdateStateToDbAction.EVENT_LEVEL_EXPRESSION, message = UpdateStateToDbAction.EVENT_MESSAGE_EXPRESSION)
        void testStateMachineEventAdapterAndEventAnnotationWithProperties(final ConnectorState from, final ConnectorState to,
                final cn.xdf.acdc.devops.core.domain.enumeration.ConnectorEvent event, final ConnectorInfoDTO connectorInfoDTO) {
        }
    }

    @Configuration
    @ComponentScan(basePackages = "cn.xdf.acdc.devops.aop",
            excludeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = SchedulerExceptionAlertAspect.class))
    @EnableAspectJAutoProxy
    static class AopConfig {

    }
}
