package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.ConnectClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorClassDO;
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
public class ConnectClusterServiceIT {

    @Autowired
    private ConnectClusterService connectClusterService;

    @Autowired
    private ConnectorClassService connectorClassService;

    @Test
    public void testSaveShouldInsertWhenNotExist() {
        ConnectClusterDO connectCluster = getConnectCluster();
        ConnectClusterDO saveResult = connectClusterService.save(connectCluster);
        Assertions.assertThat(saveResult).isEqualTo(connectCluster);
    }

    @Test
    public void testSaveShouldUpdateWhenExist() {
        ConnectClusterDO connectCluster = getConnectCluster();
        ConnectClusterDO saveResult1 = connectClusterService.save(connectCluster);
        saveResult1.setDescription("test2");
        ConnectClusterDO saveResult2 = connectClusterService.save(saveResult1);
        Assertions.assertThat(saveResult2.getDescription()).isEqualTo("test2");
        Assertions.assertThat(connectClusterService.findAll().size()).isEqualTo(1);
    }

    @Test
    public void testSaveShouldThrowExceptionWhenFieldValidateFail() {
        ConnectClusterDO connectCluster = new ConnectClusterDO();
        Throwable throwable = Assertions.catchThrowable(() -> connectClusterService.save(connectCluster));
        Assertions.assertThat(throwable).isInstanceOf(DataIntegrityViolationException.class);

        throwable = Assertions.catchThrowable(() -> connectClusterService.save(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }

    @Test
    public void testFindByIdShouldSuccess() {
        ConnectClusterDO connectCluster = getConnectCluster();
        ConnectClusterDO saveResult = connectClusterService.save(connectCluster);
        ConnectClusterDO findResult = connectClusterService.findById(saveResult.getId()).get();
        Assertions.assertThat(findResult).isEqualTo(saveResult);
        Assertions.assertThat(connectClusterService.findById(99L).isPresent()).isEqualTo(false);
    }

    private ConnectClusterDO getConnectCluster() {
        ConnectClusterDO connectCluster = new ConnectClusterDO();
        connectCluster.setConnectRestApiUrl("http://localhost:9090");
        connectCluster.setVersion("v1.0");
        connectCluster.setDescription("desc");
        return connectCluster;
    }

    @Test
    public void testFindByIdShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable = Assertions.catchThrowable(() -> connectClusterService.findById(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }

    @Test
    public void testCascade() {
        ConnectorClassDO connectorClass = new ConnectorClassDO();
        connectorClass.setName("test");
        connectorClass.setSimpleName("t");
        connectorClass.setDescription("desc");
        ConnectorClassDO connectorClassSaveResult = connectorClassService.save(connectorClass);
        ConnectClusterDO connectCluster = getConnectCluster();
        connectCluster.setConnectorClass(connectorClassSaveResult);
        ConnectClusterDO connectClusterSaveResult = connectClusterService.save(connectCluster);
        ConnectClusterDO findConnectCluster = connectClusterService.findById(connectClusterSaveResult.getId()).get();
        Assertions.assertThat(findConnectCluster.getConnectorClass()).isEqualTo(connectorClassSaveResult);
    }
}
