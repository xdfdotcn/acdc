package cn.xdf.acdc.devops.core.domain;

import cn.xdf.acdc.devops.core.domain.entity.HiveDO;
import cn.xdf.acdc.devops.core.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class HiveTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(HiveDO.class);
        HiveDO hive1 = new HiveDO();
        hive1.setId(1L);
        HiveDO hive2 = new HiveDO();
        hive2.setId(hive1.getId());
        Assertions.assertThat(hive1).isEqualTo(hive2);
        hive2.setId(2L);
        Assertions.assertThat(hive1).isNotEqualTo(hive2);
        hive1.setId(null);
        Assertions.assertThat(hive1).isNotEqualTo(hive2);
    }
}
