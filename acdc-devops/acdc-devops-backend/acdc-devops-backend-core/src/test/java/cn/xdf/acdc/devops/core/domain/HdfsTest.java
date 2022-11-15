package cn.xdf.acdc.devops.core.domain;

import cn.xdf.acdc.devops.core.domain.entity.HdfsDO;
import cn.xdf.acdc.devops.core.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class HdfsTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(HdfsDO.class);
        HdfsDO hdfs1 = new HdfsDO();
        hdfs1.setId(1L);
        HdfsDO hdfs2 = new HdfsDO();
        hdfs2.setId(hdfs1.getId());
        Assertions.assertThat(hdfs1).isEqualTo(hdfs2);
        hdfs2.setId(2L);
        Assertions.assertThat(hdfs1).isNotEqualTo(hdfs2);
        hdfs1.setId(null);
        Assertions.assertThat(hdfs1).isNotEqualTo(hdfs2);
    }
}
