package cn.xdf.acdc.devops.core.domain;

import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import cn.xdf.acdc.devops.core.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class RdbTableTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(RdbTableDO.class);
        RdbTableDO rdbTable1 = new RdbTableDO();
        rdbTable1.setId(1L);
        RdbTableDO rdbTable2 = new RdbTableDO();
        rdbTable2.setId(rdbTable1.getId());
        Assertions.assertThat(rdbTable1).isEqualTo(rdbTable2);
        rdbTable2.setId(2L);
        Assertions.assertThat(rdbTable1).isNotEqualTo(rdbTable2);
        rdbTable1.setId(null);
        Assertions.assertThat(rdbTable1).isNotEqualTo(rdbTable2);
    }
}
