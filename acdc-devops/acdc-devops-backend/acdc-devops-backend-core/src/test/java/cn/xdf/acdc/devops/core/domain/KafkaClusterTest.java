package cn.xdf.acdc.devops.core.domain;

import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class KafkaClusterTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(KafkaClusterDO.class);
        KafkaClusterDO kafkaCluster1 = new KafkaClusterDO();
        kafkaCluster1.setId(1L);
        KafkaClusterDO kafkaCluster2 = new KafkaClusterDO();
        kafkaCluster2.setId(kafkaCluster1.getId());
        Assertions.assertThat(kafkaCluster1).isEqualTo(kafkaCluster2);
        kafkaCluster2.setId(2L);
        Assertions.assertThat(kafkaCluster1).isNotEqualTo(kafkaCluster2);
        kafkaCluster1.setId(null);
        Assertions.assertThat(kafkaCluster1).isNotEqualTo(kafkaCluster2);
    }
}
