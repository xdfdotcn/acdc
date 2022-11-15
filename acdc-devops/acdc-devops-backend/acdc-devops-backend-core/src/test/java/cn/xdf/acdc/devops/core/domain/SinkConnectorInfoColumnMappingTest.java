package cn.xdf.acdc.devops.core.domain;

import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorColumnMappingDO;
import cn.xdf.acdc.devops.core.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class SinkConnectorInfoColumnMappingTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(SinkConnectorColumnMappingDO.class);
        SinkConnectorColumnMappingDO sinkConnectorColumnMapping1 = new SinkConnectorColumnMappingDO();
        sinkConnectorColumnMapping1.setId(1L);
        SinkConnectorColumnMappingDO sinkConnectorColumnMapping2 = new SinkConnectorColumnMappingDO();
        sinkConnectorColumnMapping2.setId(sinkConnectorColumnMapping1.getId());
        Assertions.assertThat(sinkConnectorColumnMapping1).isEqualTo(sinkConnectorColumnMapping2);
        sinkConnectorColumnMapping2.setId(2L);
        Assertions.assertThat(sinkConnectorColumnMapping1).isNotEqualTo(sinkConnectorColumnMapping2);
        sinkConnectorColumnMapping1.setId(null);
        Assertions.assertThat(sinkConnectorColumnMapping1).isNotEqualTo(sinkConnectorColumnMapping2);
    }
}
