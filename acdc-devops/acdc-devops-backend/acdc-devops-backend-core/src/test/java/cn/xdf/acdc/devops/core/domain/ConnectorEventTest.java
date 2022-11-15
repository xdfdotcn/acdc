package cn.xdf.acdc.devops.core.domain;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorEventDO;
import cn.xdf.acdc.devops.core.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class ConnectorEventTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(ConnectorEventDO.class);
        ConnectorEventDO connectorEvent1 = new ConnectorEventDO();
        connectorEvent1.setId(1L);
        ConnectorEventDO connectorEvent2 = new ConnectorEventDO();
        connectorEvent2.setId(connectorEvent1.getId());
        Assertions.assertThat(connectorEvent1).isEqualTo(connectorEvent2);
        connectorEvent2.setId(2L);
        Assertions.assertThat(connectorEvent1).isNotEqualTo(connectorEvent2);
        connectorEvent1.setId(null);
        Assertions.assertThat(connectorEvent1).isNotEqualTo(connectorEvent2);
    }
}
