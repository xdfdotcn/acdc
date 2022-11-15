package cn.xdf.acdc.devops.core.domain;

import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class ProjectTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(ProjectDO.class);
        ProjectDO project1 = new ProjectDO();
        project1.setId(1L);
        ProjectDO project2 = new ProjectDO();
        project2.setId(project1.getId());
        Assertions.assertThat(project1).isEqualTo(project2);
        project2.setId(2L);
        Assertions.assertThat(project1).isNotEqualTo(project2);
        project1.setId(null);
        Assertions.assertThat(project1).isNotEqualTo(project2);
    }
}
