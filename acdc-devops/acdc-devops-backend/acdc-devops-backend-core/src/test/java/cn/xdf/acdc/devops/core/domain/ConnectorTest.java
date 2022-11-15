package cn.xdf.acdc.devops.core.domain;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class ConnectorTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(ConnectorDO.class);
        ConnectorDO connector1 = new ConnectorDO();
        connector1.setId(1L);
        ConnectorDO connector2 = new ConnectorDO();
        connector2.setId(connector1.getId());
        Assertions.assertThat(connector1).isEqualTo(connector2);
        connector2.setId(2L);
        Assertions.assertThat(connector1).isNotEqualTo(connector2);
        connector1.setId(null);
        Assertions.assertThat(connector1).isNotEqualTo(connector2);
    }
}
