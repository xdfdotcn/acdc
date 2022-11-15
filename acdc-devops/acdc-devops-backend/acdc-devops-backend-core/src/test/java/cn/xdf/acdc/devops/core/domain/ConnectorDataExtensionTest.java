package cn.xdf.acdc.devops.core.domain;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDataExtensionDO;
import cn.xdf.acdc.devops.core.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class ConnectorDataExtensionTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(ConnectorDataExtensionDO.class);
        ConnectorDataExtensionDO connectorDataExtension1 = new ConnectorDataExtensionDO();
        connectorDataExtension1.setId(1L);
        ConnectorDataExtensionDO connectorDataExtension2 = new ConnectorDataExtensionDO();
        connectorDataExtension2.setId(connectorDataExtension1.getId());
        Assertions.assertThat(connectorDataExtension1).isEqualTo(connectorDataExtension2);
        connectorDataExtension2.setId(2L);
        Assertions.assertThat(connectorDataExtension1).isNotEqualTo(connectorDataExtension2);
        connectorDataExtension1.setId(null);
        Assertions.assertThat(connectorDataExtension1).isNotEqualTo(connectorDataExtension2);
    }
}
