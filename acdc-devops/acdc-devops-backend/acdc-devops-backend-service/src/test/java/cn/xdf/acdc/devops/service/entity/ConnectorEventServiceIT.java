package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorEventDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.EventLevel;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.EventReason;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.EventSource;
import cn.xdf.acdc.devops.repository.ConnectorRepository;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class ConnectorEventServiceIT {

    @Autowired
    private ConnectorEventService connectorEventService;

    @Autowired
    private ConnectorRepository connectorRepository;

    @Test
    public void testSaveShouldInsertWhenNotExist() {
        ConnectorEventDO connectorEvent = getConnectEvent();
        ConnectorEventDO saveResult = connectorEventService.save(connectorEvent);
        Assertions.assertThat(connectorEventService.findAll().get(0)).isEqualTo(saveResult);
    }

    @Test
    public void testSaveShouldUpdateWhenExist() {
        ConnectorEventDO connectorEvent = getConnectEvent();
        ConnectorEventDO saveResult1 = connectorEventService.save(connectorEvent);
        saveResult1.setReason(EventReason.CONNECTOR_ACTUAL_STATUS_CHANGED.getName());
        connectorEventService.save(saveResult1);

        Assertions.assertThat(connectorEventService.findAll().size()).isEqualTo(1);
        Assertions.assertThat(connectorEventService.findAll().get(0)).isEqualTo(saveResult1);
    }

    @Test
    public void testFindByConnectorIdShouldReturnAsExpected() {
        ConnectorDO connectorDO = getConnectorDO();
        connectorRepository.save(connectorDO);

        ConnectorEventDO connectorEvent = getConnectEvent();
        connectorEvent.setConnector(connectorDO);
        ConnectorEventDO saveResult = connectorEventService.save(connectorEvent);

        ConnectorEventDO findResult = connectorEventService.findByConnectorId(saveResult.getConnector().getId()).get(0);
        Assertions.assertThat(findResult).isEqualTo(saveResult);
        Assertions.assertThat(connectorEventService.findByConnectorId(99L)).isNullOrEmpty();
    }

    private ConnectorEventDO getConnectEvent() {
        ConnectorEventDO connectorEvent = new ConnectorEventDO();
        connectorEvent.setId(1L);
        connectorEvent.setConnector(new ConnectorDO().setId(1L));
        connectorEvent.setSource(EventSource.USER_OPERATION);
        connectorEvent.setLevel(EventLevel.DEBUG);
        connectorEvent.setReason(EventReason.CONNECTOR_ACTUAL_STATUS_CHANGED.getName());
        connectorEvent.setMessage("message_1");
        return connectorEvent;
    }

    private ConnectorDO getConnectorDO() {
        return ConnectorDO.builder()
                .name("test_connector")
                .build();
    }
}
