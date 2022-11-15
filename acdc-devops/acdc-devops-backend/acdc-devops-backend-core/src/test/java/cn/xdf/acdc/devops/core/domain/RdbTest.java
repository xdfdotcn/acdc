package cn.xdf.acdc.devops.core.domain;

import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class RdbTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(RdbDO.class);
        RdbDO rdb1 = new RdbDO();
        rdb1.setId(1L);
        RdbDO rdb2 = new RdbDO();
        rdb2.setId(rdb1.getId());
        Assertions.assertThat(rdb1).isEqualTo(rdb2);
        rdb2.setId(2L);
        Assertions.assertThat(rdb1).isNotEqualTo(rdb2);
        rdb1.setId(null);
        Assertions.assertThat(rdb1).isNotEqualTo(rdb2);
    }
}
