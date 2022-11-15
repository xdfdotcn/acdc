package cn.xdf.acdc.devops.core.domain;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorClassDO;
import cn.xdf.acdc.devops.core.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class ConnectorClassTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(ConnectorClassDO.class);
        ConnectorClassDO connectorClass1 = new ConnectorClassDO();
        connectorClass1.setId(1L);
        ConnectorClassDO connectorClass2 = new ConnectorClassDO();
        connectorClass2.setId(connectorClass1.getId());
        Assertions.assertThat(connectorClass1).isEqualTo(connectorClass2);
        connectorClass2.setId(2L);
        Assertions.assertThat(connectorClass1).isNotEqualTo(connectorClass2);
        connectorClass1.setId(null);
        Assertions.assertThat(connectorClass1).isNotEqualTo(connectorClass2);
    }
}
