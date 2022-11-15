package cn.xdf.acdc.devops.core.domain;

import cn.xdf.acdc.devops.core.domain.entity.ConnectClusterDO;
import cn.xdf.acdc.devops.core.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class ConnectClusterTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(ConnectClusterDO.class);
        ConnectClusterDO connectCluster1 = new ConnectClusterDO();
        connectCluster1.setId(1L);
        ConnectClusterDO connectCluster2 = new ConnectClusterDO();
        connectCluster2.setId(connectCluster1.getId());
        Assertions.assertThat(connectCluster1).isEqualTo(connectCluster2);
        connectCluster2.setId(2L);
        Assertions.assertThat(connectCluster1).isNotEqualTo(connectCluster2);
        connectCluster1.setId(null);
        Assertions.assertThat(connectCluster1).isNotEqualTo(connectCluster2);
    }
}
