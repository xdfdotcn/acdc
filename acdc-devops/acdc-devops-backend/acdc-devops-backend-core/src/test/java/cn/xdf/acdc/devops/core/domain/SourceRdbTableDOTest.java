package cn.xdf.acdc.devops.core.domain;

import cn.xdf.acdc.devops.core.domain.entity.SourceRdbTableDO;
import cn.xdf.acdc.devops.core.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class SourceRdbTableDOTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(SourceRdbTableDO.class);
        SourceRdbTableDO sourceRdbTable1 = new SourceRdbTableDO();
        sourceRdbTable1.setId(1L);
        SourceRdbTableDO sourceRdbTable2 = new SourceRdbTableDO();
        sourceRdbTable2.setId(sourceRdbTable1.getId());
        Assertions.assertThat(sourceRdbTable1).isEqualTo(sourceRdbTable2);
        sourceRdbTable2.setId(2L);
        Assertions.assertThat(sourceRdbTable1).isNotEqualTo(sourceRdbTable2);
        sourceRdbTable1.setId(null);
        Assertions.assertThat(sourceRdbTable1).isNotEqualTo(sourceRdbTable2);
    }
}
