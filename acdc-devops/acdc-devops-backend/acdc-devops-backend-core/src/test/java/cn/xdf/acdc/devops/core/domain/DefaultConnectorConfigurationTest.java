package cn.xdf.acdc.devops.core.domain;

import cn.xdf.acdc.devops.core.domain.entity.DefaultConnectorConfigurationDO;
import cn.xdf.acdc.devops.core.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class DefaultConnectorConfigurationTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(DefaultConnectorConfigurationDO.class);
        DefaultConnectorConfigurationDO defaultConnectorConfiguration1 = new DefaultConnectorConfigurationDO();
        defaultConnectorConfiguration1.setId(1L);
        DefaultConnectorConfigurationDO defaultConnectorConfiguration2 = new DefaultConnectorConfigurationDO();
        defaultConnectorConfiguration2.setId(defaultConnectorConfiguration1.getId());
        Assertions.assertThat(defaultConnectorConfiguration1).isEqualTo(defaultConnectorConfiguration2);
        defaultConnectorConfiguration2.setId(2L);
        Assertions.assertThat(defaultConnectorConfiguration1).isNotEqualTo(defaultConnectorConfiguration2);
        defaultConnectorConfiguration1.setId(null);
        Assertions.assertThat(defaultConnectorConfiguration1).isNotEqualTo(defaultConnectorConfiguration2);
    }
}
