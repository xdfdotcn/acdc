package cn.xdf.acdc.devops.service.aop;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorEventDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.EventLevel;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.EventReason;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.EventSource;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import cn.xdf.acdc.devops.service.entity.ConnectorEventService;
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
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.ResourceAccessException;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {EventAspectT.class, EventAspectT.AopConfig.class})
public class EventAspectT {

    @Autowired
    private EventAspectAopTest eventAspectAopTest;

    @MockBean
    private ConnectorEventService connectorEventService;

    @Test
    public void testWithoutEventAnnotation() {
        eventAspectAopTest.testWithoutEventAnnotation();
        ArgumentCaptor<ConnectorEventDO> eventCaptor = ArgumentCaptor.forClass(ConnectorEventDO.class);
        Mockito.verify(connectorEventService, Mockito.never()).save(eventCaptor.capture());
    }

    @Test
    public void testEventAnnotationWithValues() {
        eventAspectAopTest.testEventAnnotationWithValues();
        ArgumentCaptor<ConnectorEventDO> eventCaptor = ArgumentCaptor.forClass(ConnectorEventDO.class);
        Mockito.verify(connectorEventService).save(eventCaptor.capture());
        ConnectorEventDO event = eventCaptor.getValue();
        Assertions.assertThat(event.getReason()).isEqualTo(EventReason.CONNECTOR_ACTUAL_STATUS_CHANGED.getName());
        Assertions.assertThat(event.getLevel()).isEqualTo(EventLevel.INFO);
        Assertions.assertThat(event.getConnector().getId()).isEqualTo(1L);
        Assertions.assertThat(event.getSource()).isEqualTo(EventSource.ACDC_SCHEDULER);
        Assertions.assertThat(event.getMessage()).isEqualTo("");
    }

    @Test
    public void testEventAnnotationWithProperties() {
        ConnectorInfoDTO connectorInfoDTO = new ConnectorInfoDTO();
        connectorInfoDTO.setId(1L);
        connectorInfoDTO.setRemark("exception xxx");
        eventAspectAopTest.testEventAnnotationWithProperties(ConnectorState.PENDING, ConnectorState.CREATION_FAILED,
                cn.xdf.acdc.devops.core.domain.enumeration.ConnectorEvent.CREATE_FAILURE, connectorInfoDTO);
        ArgumentCaptor<ConnectorEventDO> eventCaptor = ArgumentCaptor.forClass(ConnectorEventDO.class);
        Mockito.verify(connectorEventService).save(eventCaptor.capture());
        ConnectorEventDO event = eventCaptor.getValue();
        Assertions.assertThat(event.getLevel()).isEqualTo(EventLevel.INFO);
        Assertions.assertThat(event.getReason()).isEqualTo(EventReason.CONNECTOR_ACTUAL_STATUS_CHANGED.getName());
        Assertions.assertThat(event.getConnector().getId()).isEqualTo(1L);
        Assertions.assertThat(event.getSource()).isEqualTo(EventSource.ACDC_SCHEDULER);
        Assertions.assertThat(event.getMessage()).isEqualTo("exception xxx");
    }

    @Test
    public void testEventAnnotationWithExceptions() {
        ConnectorInfoDTO connectorInfoDTO = new ConnectorInfoDTO();
        connectorInfoDTO.setId(1L);
        connectorInfoDTO.setRemark("exception xxx");
        try {
            eventAspectAopTest.testEventAnnotationWithExceptions(ConnectorState.PENDING, ConnectorState.CREATION_FAILED,
                    cn.xdf.acdc.devops.core.domain.enumeration.ConnectorEvent.CREATE_FAILURE, connectorInfoDTO);
        } catch (ResourceAccessException e) {
            Assertions.assertThat(e.toString()).isEqualTo("org.springframework.web.client.ResourceAccessException: ResourceAccessException xxx");
        }

        ArgumentCaptor<ConnectorEventDO> eventCaptor = ArgumentCaptor.forClass(ConnectorEventDO.class);
        Mockito.verify(connectorEventService).save(eventCaptor.capture());
        ConnectorEventDO event = eventCaptor.getValue();
        Assertions.assertThat(event.getLevel()).isEqualTo(EventLevel.ERROR);
        Assertions.assertThat(event.getReason()).isEqualTo(EventReason.EXECUTION_ERROR.getName());
        Assertions.assertThat(event.getConnector().getId()).isEqualTo(1L);
        Assertions.assertThat(event.getSource()).isEqualTo(EventSource.ACDC_SCHEDULER);
        Assertions.assertThat(event.getMessage()).startsWith("org.springframework.web.client.ResourceAccessException: ResourceAccessException xxx");
    }

    @Component
    static class EventAspectAopTest {

        void testWithoutEventAnnotation() {

        }

        @Event(connectorId = "'1'", reason = EventReason.CONNECTOR_ACTUAL_STATUS_CHANGED, source = EventSource.ACDC_SCHEDULER)
        void testEventAnnotationWithValues() {

        }

        @Event(connectorId = "#connectorInfoDTO.id", reason = EventReason.CONNECTOR_ACTUAL_STATUS_CHANGED, source = EventSource.ACDC_SCHEDULER, message = "#connectorInfoDTO.remark")
        void testEventAnnotationWithProperties(final ConnectorState from, final ConnectorState to,
                final cn.xdf.acdc.devops.core.domain.enumeration.ConnectorEvent event, final ConnectorInfoDTO connectorInfoDTO) {

        }

        @Event(connectorId = "#connectorInfoDTO.id", reason = EventReason.CONNECTOR_ACTUAL_STATUS_CHANGED, source = EventSource.ACDC_SCHEDULER, message = "#connectorInfoDTO.remark")
        void testEventAnnotationWithExceptions(final ConnectorState from, final ConnectorState to,
                final cn.xdf.acdc.devops.core.domain.enumeration.ConnectorEvent event, final ConnectorInfoDTO connectorInfoDTO) {

            throw new ResourceAccessException("ResourceAccessException xxx");
        }

    }

    @Configuration
    @ComponentScan({"cn.xdf.acdc.devops.service.aop"})
    @EnableAspectJAutoProxy
    static class AopConfig {

    }
}
