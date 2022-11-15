package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorClassDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class ConnectorClassServiceIT {

    @Autowired
    private ConnectorClassService connectorClassService;

    @Test
    public void testSaveShouldInsertWhenNotExist() {
        ConnectorClassDO connectorClass = getConnectorClass();
        ConnectorClassDO saveResult = connectorClassService.save(connectorClass);
        Assertions.assertThat(saveResult).isEqualTo(connectorClass);
    }

    @Test
    public void testSaveShouldUpdateWhenExist() {
        ConnectorClassDO connectorClass = getConnectorClass();
        ConnectorClassDO saveResult1 = connectorClassService.save(connectorClass);
        saveResult1.setName("test2");
        ConnectorClassDO saveResult2 = connectorClassService.save(saveResult1);
        Assertions.assertThat(saveResult2.getName()).isEqualTo("test2");
        Assertions.assertThat(connectorClassService.findAll().size()).isEqualTo(1);
    }

    @Test
    public void testSaveShouldThrowExceptionWhenFieldValidateFail() {
        ConnectorClassDO connectorClass = new ConnectorClassDO();
        Throwable throwable = Assertions.catchThrowable(() -> connectorClassService.save(connectorClass));
        Assertions.assertThat(throwable).isInstanceOf(DataIntegrityViolationException.class);

        throwable = Assertions.catchThrowable(() -> connectorClassService.save(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }

    @Test
    public void testFindByIdShouldSuccess() {
        ConnectorClassDO connectorClass = getConnectorClass();
        ConnectorClassDO saveResult = connectorClassService.save(connectorClass);
        ConnectorClassDO findResult = connectorClassService.findById(saveResult.getId()).get();
        Assertions.assertThat(findResult).isEqualTo(saveResult);
        Assertions.assertThat(connectorClassService.findById(99L).isPresent()).isEqualTo(false);
    }

    private ConnectorClassDO getConnectorClass() {
        ConnectorClassDO connectorClass = new ConnectorClassDO();
        connectorClass.setName("test");
        connectorClass.setSimpleName("t");
        connectorClass.setDescription("desc");
        connectorClass.setConnectorType(ConnectorType.SINK);
        connectorClass.setDataSystemType(DataSystemType.KAFKA);
        return connectorClass;
    }

    @Test
    public void testFindByIdShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable = Assertions.catchThrowable(() -> connectorClassService.findById(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }
}
