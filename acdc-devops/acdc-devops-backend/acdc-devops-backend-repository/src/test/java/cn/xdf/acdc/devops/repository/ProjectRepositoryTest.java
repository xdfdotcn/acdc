package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.MetadataSourceType;
import cn.xdf.acdc.devops.core.domain.query.ProjectQuery;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ProjectRepositoryTest {

    @Autowired
    private ProjectRepository projectRepository;

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testFindBySourceShouldPass() {
        projectRepository.findBySource(MetadataSourceType.FROM_PANDORA);
    }

    @Test
    public void testQueryShouldPass() {
        ProjectQuery projectQuery = new ProjectQuery();
        projectQuery.setDeleted(false);
        projectRepository.query(projectQuery);
    }
}
