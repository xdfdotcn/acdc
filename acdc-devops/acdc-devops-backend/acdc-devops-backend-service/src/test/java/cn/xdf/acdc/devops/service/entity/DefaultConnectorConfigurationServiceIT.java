package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorClassDO;
import cn.xdf.acdc.devops.core.domain.entity.DefaultConnectorConfigurationDO;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.ConstraintViolationException;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class DefaultConnectorConfigurationServiceIT {

    @Autowired
    private DefaultConnectorConfigurationService defaultConnectorConfigurationService;

    @Autowired
    private ConnectorClassService connectorClassService;

    @Test
    public void testSaveShouldInsertWhenNotExist() {
        DefaultConnectorConfigurationDO defaultConnectorConfiguration = getDefaultConnectorConfiguration();
        DefaultConnectorConfigurationDO saveResult = defaultConnectorConfigurationService.save(defaultConnectorConfiguration);
        Assertions.assertThat(saveResult).isEqualTo(defaultConnectorConfiguration);
    }

    @Test
    public void testSaveShouldUpdateWhenExist() {
        DefaultConnectorConfigurationDO defaultConnectorConfiguration = getDefaultConnectorConfiguration();
        DefaultConnectorConfigurationDO saveResult1 = defaultConnectorConfigurationService.save(defaultConnectorConfiguration);
        saveResult1.setName("test2");
        DefaultConnectorConfigurationDO saveResult2 = defaultConnectorConfigurationService.save(saveResult1);
        Assertions.assertThat(saveResult2.getName()).isEqualTo("test2");
        Assertions.assertThat(defaultConnectorConfigurationService.findAll().size()).isEqualTo(1);
    }

    @Test
    public void testSaveShouldThrowExceptionWhenFieldValidateFail() {
        DefaultConnectorConfigurationDO defaultConnectorConfiguration = new DefaultConnectorConfigurationDO();
        Throwable throwable = Assertions.catchThrowable(() -> defaultConnectorConfigurationService.save(defaultConnectorConfiguration));
        Assertions.assertThat(throwable).isInstanceOf(ConstraintViolationException.class);

        throwable = Assertions.catchThrowable(() -> defaultConnectorConfigurationService.save(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }

    @Test
    public void testFindByIdShouldSuccess() {
        DefaultConnectorConfigurationDO defaultConnectorConfiguration = getDefaultConnectorConfiguration();
        DefaultConnectorConfigurationDO saveResult = defaultConnectorConfigurationService.save(defaultConnectorConfiguration);
        DefaultConnectorConfigurationDO findResult = defaultConnectorConfigurationService.findById(saveResult.getId()).get();
        Assertions.assertThat(findResult).isEqualTo(saveResult);
        Assertions.assertThat(defaultConnectorConfigurationService.findById(99L).isPresent()).isEqualTo(false);
    }

    private DefaultConnectorConfigurationDO getDefaultConnectorConfiguration() {
        DefaultConnectorConfigurationDO defaultConnectorConfiguration = new DefaultConnectorConfigurationDO();
        defaultConnectorConfiguration.setName("task.id");
        defaultConnectorConfiguration.setValue("task-1");
        return defaultConnectorConfiguration;
    }

    @Test
    public void testFindByIdShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable = Assertions.catchThrowable(() -> defaultConnectorConfigurationService.findById(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }

    @Test
    public void testCascade() {
        ConnectorClassDO connectorClass = new ConnectorClassDO();
        connectorClass.setName("test");
        connectorClass.setSimpleName("t");
        connectorClass.setDescription("desc");
        ConnectorClassDO connectorClassSaveResult = connectorClassService.save(connectorClass);
        DefaultConnectorConfigurationDO defaultConnectorConfiguration = getDefaultConnectorConfiguration();
        defaultConnectorConfiguration.setConnectorClass(connectorClassSaveResult);
        DefaultConnectorConfigurationDO defaultConnectorConfigurationSaveResult = defaultConnectorConfigurationService.save(defaultConnectorConfiguration);
        DefaultConnectorConfigurationDO findDefaultConnectorConfiguration = defaultConnectorConfigurationService.findById(defaultConnectorConfigurationSaveResult.getId()).get();
        Assertions.assertThat(findDefaultConnectorConfiguration.getConnectorClass()).isEqualTo(connectorClassSaveResult);
    }
}
