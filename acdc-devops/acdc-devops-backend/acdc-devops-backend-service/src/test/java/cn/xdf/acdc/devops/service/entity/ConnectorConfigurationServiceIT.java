package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.ConstraintViolationException;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class ConnectorConfigurationServiceIT {

    @Autowired
    private ConnectorConfigurationService connectorConfigurationService;

    @Autowired
    private ConnectorService connectorService;

    @Test
    public void testSaveShouldInsertWhenNotExist() {
        ConnectorConfigurationDO connectorConfiguration = createConnectorConfiguration();
        ConnectorConfigurationDO saveResult = connectorConfigurationService.save(connectorConfiguration);
        Assertions.assertThat(saveResult).isEqualTo(connectorConfiguration);
    }

    @Test
    public void testSaveShouldUpdateWhenExist() {
        ConnectorConfigurationDO connectorConfiguration = createConnectorConfiguration();
        ConnectorConfigurationDO saveResult1 = connectorConfigurationService.save(connectorConfiguration);
        saveResult1.setName("test2");
        ConnectorConfigurationDO saveResult2 = connectorConfigurationService.save(saveResult1);
        Assertions.assertThat(saveResult2.getName()).isEqualTo("test2");
        Assertions.assertThat(connectorConfigurationService.findAll().size()).isEqualTo(1);
    }

    @Test
    public void testSaveShouldThrowExceptionWhenFieldValidateFail() {
        ConnectorConfigurationDO connectorConfiguration = new ConnectorConfigurationDO();
        Throwable throwable = Assertions.catchThrowable(() -> connectorConfigurationService.save(connectorConfiguration));
        Assertions.assertThat(throwable).isInstanceOf(ConstraintViolationException.class);

        throwable = Assertions.catchThrowable(() -> connectorConfigurationService.save(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }

    @Test
    public void testSaveAllShouldInsertWhenNotExist() {
        List<ConnectorConfigurationDO> connectorConfigurationList = createConnectorConfigurationList();
        List<ConnectorConfigurationDO> saveResultList = connectorConfigurationService.saveAll(connectorConfigurationList);

        Assertions.assertThat(saveResultList.size()).isEqualTo(connectorConfigurationList.size());

        for (int i = 0; i < connectorConfigurationList.size(); i++) {
            connectorConfigurationList.get(i).setId(saveResultList.get(i).getId());
            Assertions.assertThat(saveResultList.get(i)).isEqualTo(connectorConfigurationList.get(i));
        }
    }

    @Test
    public void testSaveAllShouldUpdateWhenAlreadyExist() {
        List<ConnectorConfigurationDO> connectorConfigurationList = createConnectorConfigurationList();
        List<ConnectorConfigurationDO> saveResultList = connectorConfigurationService.saveAll(connectorConfigurationList);
        saveResultList.forEach(t -> t.setName("test_update"));
        connectorConfigurationService.saveAll(saveResultList).forEach(t -> {
            Assertions.assertThat(t.getName()).isEqualTo("test_update");
        });
        Assertions.assertThat(connectorConfigurationService.saveAll(saveResultList).size()).isEqualTo(connectorConfigurationList.size());
    }

    @Test
    public void testSaveAllShouldDoNothingWhenGivenEmptyCollection() {
        connectorConfigurationService.saveAll(Lists.newArrayList());
        Assertions.assertThat(connectorConfigurationService.findAll().size()).isEqualTo(0);
    }

    @Test
    public void testSaveAllShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable = Assertions.catchThrowable(() -> connectorConfigurationService.saveAll(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }

    @Test
    public void testFindByIdShouldSuccess() {
        ConnectorConfigurationDO connectorConfiguration = createConnectorConfiguration();
        ConnectorConfigurationDO saveResult = connectorConfigurationService.save(connectorConfiguration);
        ConnectorConfigurationDO findResult = connectorConfigurationService.findById(saveResult.getId()).get();
        Assertions.assertThat(findResult).isEqualTo(saveResult);
        Assertions.assertThat(connectorConfigurationService.findById(99L).isPresent()).isEqualTo(false);
    }

    private ConnectorConfigurationDO createConnectorConfiguration() {
        ConnectorConfigurationDO connectorConfiguration = new ConnectorConfigurationDO();
        connectorConfiguration.setName("name-test");
        connectorConfiguration.setValue("value-test");
        return connectorConfiguration;
    }

    private List<ConnectorConfigurationDO> createConnectorConfigurationList() {
        List<ConnectorConfigurationDO> connectorConfigurationList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            ConnectorConfigurationDO connectorConfiguration = new ConnectorConfigurationDO();
            connectorConfiguration.setName("name" + i);
            connectorConfiguration.setValue("value" + i);
            connectorConfigurationList.add(connectorConfiguration);
        }
        return connectorConfigurationList;
    }

    @Test
    public void testFindByIdShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable = Assertions.catchThrowable(() -> connectorConfigurationService.findById(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }

    @Test
    public void testCascade() {
        ConnectorDO connector = new ConnectorDO();
        connector.setName("test");
        ConnectorDO connectorSaveResult = connectorService.save(connector);

        ConnectorConfigurationDO connectorConfiguration = new ConnectorConfigurationDO();
        connectorConfiguration.setName("name-test");
        connectorConfiguration.setValue("value-test");
        connectorConfiguration.setConnector(connectorSaveResult);
        ConnectorConfigurationDO connectorConfigurationSaveResult = connectorConfigurationService
                .save(connectorConfiguration);
        ConnectorConfigurationDO findResult = connectorConfigurationService
                .findById(connectorConfigurationSaveResult.getId()).get();
        Assertions.assertThat(findResult.getConnector()).isEqualTo(connectorSaveResult);
    }
}
