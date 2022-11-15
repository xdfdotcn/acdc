package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDataExtensionDO;
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
public class ConnectorDataExtensionServiceIT {

    @Autowired
    private ConnectorDataExtensionService connectorDataExtensionService;

    @Autowired
    private ConnectorClassService connectorClassService;

    @Test
    public void testSaveShouldInsertWhenNotExist() {
        ConnectorDataExtensionDO connectorDataExtension = createConnectorDataExtension();
        ConnectorDataExtensionDO saveResult = connectorDataExtensionService.save(connectorDataExtension);
        Assertions.assertThat(saveResult).isEqualTo(connectorDataExtension);
    }

    @Test
    public void testSaveShouldUpdateWhenExist() {
        ConnectorDataExtensionDO connectorDataExtension = createConnectorDataExtension();
        ConnectorDataExtensionDO saveResult1 = connectorDataExtensionService.save(connectorDataExtension);
        saveResult1.setName("test2");
        ConnectorDataExtensionDO saveResult2 = connectorDataExtensionService.save(saveResult1);
        Assertions.assertThat(saveResult2.getName()).isEqualTo("test2");
        Assertions.assertThat(connectorDataExtensionService.findAll().size()).isEqualTo(1);
    }

    @Test
    public void testSaveShouldThrowExceptionWhenFieldValidateFail() {
        ConnectorDataExtensionDO connectorDataExtension = new ConnectorDataExtensionDO();
        Throwable throwable = Assertions.catchThrowable(() -> connectorDataExtensionService.save(connectorDataExtension));
        Assertions.assertThat(throwable).isInstanceOf(ConstraintViolationException.class);

        throwable = Assertions.catchThrowable(() -> connectorDataExtensionService.save(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }

    @Test
    public void testSaveAllShouldInsertWhenNotExist() {
        List<ConnectorDataExtensionDO> connectorDataExtensionList = createConnectorDataExtensionList();
        List<ConnectorDataExtensionDO> saveResultList = connectorDataExtensionService.saveAll(connectorDataExtensionList);

        Assertions.assertThat(saveResultList.size()).isEqualTo(connectorDataExtensionList.size());

        for (int i = 0; i < connectorDataExtensionList.size(); i++) {
            connectorDataExtensionList.get(i).setId(saveResultList.get(i).getId());
            Assertions.assertThat(saveResultList.get(i)).isEqualTo(connectorDataExtensionList.get(i));
        }
    }

    @Test
    public void testSaveAllShouldUpdateWhenAlreadyExist() {
        List<ConnectorDataExtensionDO> connectorDataExtensionList = createConnectorDataExtensionList();
        List<ConnectorDataExtensionDO> saveResultList = connectorDataExtensionService.saveAll(connectorDataExtensionList);
        saveResultList.forEach(t -> t.setName("test_update"));
        connectorDataExtensionService.saveAll(saveResultList).forEach(t -> {
            Assertions.assertThat(t.getName()).isEqualTo("test_update");
        });
        Assertions.assertThat(connectorDataExtensionService.saveAll(saveResultList).size()).isEqualTo(connectorDataExtensionList.size());
    }

    @Test
    public void testSaveAllShouldDoNothingWhenGivenEmptyCollection() {
        connectorDataExtensionService.saveAll(Lists.newArrayList());
        Assertions.assertThat(connectorDataExtensionService.findAll().size()).isEqualTo(0);
    }

    @Test
    public void testSaveAllShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable = Assertions.catchThrowable(() -> connectorDataExtensionService.saveAll(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }

    @Test
    public void testFindByIdShouldSuccess() {
        ConnectorDataExtensionDO connectorDataExtension = createConnectorDataExtension();
        ConnectorDataExtensionDO saveResult = connectorDataExtensionService.save(connectorDataExtension);
        ConnectorDataExtensionDO findResult = connectorDataExtensionService.findById(saveResult.getId()).get();
        Assertions.assertThat(findResult).isEqualTo(saveResult);
        Assertions.assertThat(connectorDataExtensionService.findById(99L).isPresent()).isEqualTo(false);
    }

    private ConnectorDataExtensionDO createConnectorDataExtension() {
        ConnectorDataExtensionDO connectorDataExtension = new ConnectorDataExtensionDO();
        connectorDataExtension.setName("name");
        connectorDataExtension.setValue("value");
        return connectorDataExtension;
    }

    private List<ConnectorDataExtensionDO> createConnectorDataExtensionList() {
        List<ConnectorDataExtensionDO> connectorDataExtensionList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            ConnectorDataExtensionDO connectorDataExtension = new ConnectorDataExtensionDO();
            connectorDataExtension.setName("name" + i);
            connectorDataExtension.setValue("value" + i);
            connectorDataExtensionList.add(connectorDataExtension);
        }
        return connectorDataExtensionList;
    }

    @Test
    public void testFindByIdShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable = Assertions.catchThrowable(() -> connectorDataExtensionService.findById(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }
}
