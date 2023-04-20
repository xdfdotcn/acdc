package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.HashSet;
import java.util.Set;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class ConnectorRepositoryT {

    @Autowired
    private ConnectorRepository connectorRepository;

    @PersistenceContext
    private EntityManager entityManager;

    @Test
    public void testCascadeUpdate() {
        ConnectorDO connectorDO = new ConnectorDO();
        connectorDO.setName("test-cascade-update");
        connectorDO.setDataSystemResource(new DataSystemResourceDO(1L));
        ConnectorConfigurationDO connectorConfigurationDO = new ConnectorConfigurationDO();
        connectorConfigurationDO.setName("tables");
        connectorConfigurationDO.setValue("tb_1");
        Set<ConnectorConfigurationDO> configs = new HashSet<>();
        configs.add(connectorConfigurationDO);
        connectorDO.setConnectorConfigurations(configs);
        connectorRepository.save(connectorDO);

        entityManager.flush();
        entityManager.clear();
        ConnectorDO updatedConnector = connectorRepository.findAll().get(0);
        updatedConnector.getConnectorConfigurations().stream().findFirst().get().setValue("tb_1, tb_2");
        connectorRepository.save(updatedConnector);
        Assert.assertEquals("tb_1, tb_2",
                connectorRepository.findAll().get(0).getConnectorConfigurations()
                        .stream().findFirst().get().getValue()
        );
        entityManager.flush();
        entityManager.clear();
        Assert.assertEquals("tb_1, tb_2",
                connectorRepository.findAll().get(0).getConnectorConfigurations()
                        .stream().findFirst().get().getValue()
        );
    }
}
