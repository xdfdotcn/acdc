package cn.xdf.acdc.devops.core.domain;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorConfigurationDO;
import cn.xdf.acdc.devops.core.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class ConnectorConfigurationTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(ConnectorConfigurationDO.class);
        ConnectorConfigurationDO connectorConfiguration1 = new ConnectorConfigurationDO();
        connectorConfiguration1.setId(1L);
        ConnectorConfigurationDO connectorConfiguration2 = new ConnectorConfigurationDO();
        connectorConfiguration2.setId(connectorConfiguration1.getId());
        Assertions.assertThat(connectorConfiguration1).isEqualTo(connectorConfiguration2);
        connectorConfiguration2.setId(2L);
        Assertions.assertThat(connectorConfiguration1).isNotEqualTo(connectorConfiguration2);
        connectorConfiguration1.setId(null);
        Assertions.assertThat(connectorConfiguration1).isNotEqualTo(connectorConfiguration2);
    }
}
