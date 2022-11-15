package cn.xdf.acdc.devops.core.domain;

import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.core.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class RdbDatabaseTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(RdbDatabaseDO.class);
        RdbDatabaseDO rdbDatabase1 = new RdbDatabaseDO();
        rdbDatabase1.setId(1L);
        RdbDatabaseDO rdbDatabase2 = new RdbDatabaseDO();
        rdbDatabase2.setId(rdbDatabase1.getId());
        Assertions.assertThat(rdbDatabase1.toString()).isEqualTo(rdbDatabase2.toString());
//        rdbDatabase2.setId(2L);
//        Assertions.assertThat(rdbDatabase1).isNotEqualTo(rdbDatabase2);
//        rdbDatabase1.setId(null);
//        Assertions.assertThat(rdbDatabase1).isNotEqualTo(rdbDatabase2);
    }
}
