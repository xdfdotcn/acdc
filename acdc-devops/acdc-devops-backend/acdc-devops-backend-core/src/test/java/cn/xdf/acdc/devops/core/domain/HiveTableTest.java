package cn.xdf.acdc.devops.core.domain;

import cn.xdf.acdc.devops.core.domain.entity.HiveTableDO;
import cn.xdf.acdc.devops.core.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class HiveTableTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(HiveTableDO.class);
        HiveTableDO hiveTable1 = new HiveTableDO();
        hiveTable1.setId(1L);
        HiveTableDO hiveTable2 = new HiveTableDO();
        hiveTable2.setId(hiveTable1.getId());
        Assertions.assertThat(hiveTable1).isEqualTo(hiveTable2);
        hiveTable2.setId(2L);
        Assertions.assertThat(hiveTable1).isNotEqualTo(hiveTable2);
        hiveTable1.setId(null);
        Assertions.assertThat(hiveTable1).isNotEqualTo(hiveTable2);
    }
}
