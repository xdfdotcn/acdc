package cn.xdf.acdc.devops.core.domain;

import cn.xdf.acdc.devops.core.domain.entity.HiveDatabaseDO;
import cn.xdf.acdc.devops.core.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class HiveDatabaseTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(HiveDatabaseDO.class);
        HiveDatabaseDO hiveDatabase1 = new HiveDatabaseDO();
        hiveDatabase1.setId(1L);
        HiveDatabaseDO hiveDatabase2 = new HiveDatabaseDO();
        hiveDatabase2.setId(hiveDatabase1.getId());
        Assertions.assertThat(hiveDatabase1).isEqualTo(hiveDatabase2);
        hiveDatabase2.setId(2L);
        Assertions.assertThat(hiveDatabase1).isNotEqualTo(hiveDatabase2);
        hiveDatabase1.setId(null);
        Assertions.assertThat(hiveDatabase1).isNotEqualTo(hiveDatabase2);
    }
}
