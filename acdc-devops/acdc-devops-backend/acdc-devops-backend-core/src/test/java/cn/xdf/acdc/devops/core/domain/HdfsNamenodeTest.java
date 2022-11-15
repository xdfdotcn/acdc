package cn.xdf.acdc.devops.core.domain;

import cn.xdf.acdc.devops.core.domain.entity.HdfsNamenodeDO;
import cn.xdf.acdc.devops.core.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class HdfsNamenodeTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(HdfsNamenodeDO.class);
        HdfsNamenodeDO hdfsNamenode1 = new HdfsNamenodeDO();
        hdfsNamenode1.setId(1L);
        HdfsNamenodeDO hdfsNamenode2 = new HdfsNamenodeDO();
        hdfsNamenode2.setId(hdfsNamenode1.getId());
        Assertions.assertThat(hdfsNamenode1).isEqualTo(hdfsNamenode2);
        hdfsNamenode2.setId(2L);
        Assertions.assertThat(hdfsNamenode1).isNotEqualTo(hdfsNamenode2);
        hdfsNamenode1.setId(null);
        Assertions.assertThat(hdfsNamenode1).isNotEqualTo(hdfsNamenode2);
    }
}
