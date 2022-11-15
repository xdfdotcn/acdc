package cn.xdf.acdc.devops.service.process.datasystem.rdb.impl;

import cn.xdf.acdc.devops.core.domain.dto.RdbDTO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ProjectSourceType;
import cn.xdf.acdc.devops.repository.ProjectRepository;
import cn.xdf.acdc.devops.repository.RdbRepository;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.RdbProcessService;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.transaction.Transactional;

// CHECKSTYLE:OFF

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class RdbProcessServiceImplTest {

    @Autowired
    private RdbProcessService rdbProcessService;

    @Autowired
    private RdbRepository rdbRepository;

    @Autowired
    private ProjectRepository projectRepository;

    @Before
    public void setUp() {

    }

    @Test
    public void testGetRdb() {
        RdbDO rdbDO = mockRdb();
        RdbDTO rdb = rdbProcessService.getRdb(rdbDO.getId());
        Assertions.assertThat(rdb).isNotNull();
    }

    @Test
    public void testSaveRdb() {
        RdbDO rdbDO = mockRdb();

        RdbDTO rdbDTO = RdbDTO.builder()
                .name("test-save-rdb")
                .rdbType("mysql")
                .username("admin")
                .desc("for test")
                .password("admin123.")
                .projectId(rdbDO.getProjects().stream().findFirst().get().getId())
                .build();

        rdbProcessService.saveRdb(rdbDTO);
        RdbDTO rdb = rdbProcessService.getRdb(rdbDTO.getId());
        Assertions.assertThat(rdb.getName()).isEqualTo("test-save-rdb");
    }

    @Test
    public void testUpdateRdb() {
        RdbDO rdbDO = mockRdb();

        RdbDTO rdbDTO = RdbDTO.builder().name("测试-2").rdbType("mysql").id(rdbDO.getId()).build();
        rdbProcessService.updateRdb(rdbDTO);
        RdbDTO rdb = rdbProcessService.getRdb(rdbDO.getId());
        Assertions.assertThat(rdb.getName()).isEqualTo("测试-2");
    }

    private RdbDO mockRdb() {
        ProjectDO projectDO = new ProjectDO();
        projectDO.setName("测试项目");
        projectDO.setDescription("Test");
        projectDO.setSource(ProjectSourceType.USER_INPUT);
        projectDO.setOriginalId(1L);
        projectRepository.save(projectDO);

        RdbDO rdbDO = new RdbDO();
        rdbDO.setName("测试");
        rdbDO.setRdbType("mysql");
        rdbDO.setUsername("root");
        rdbDO.setPassword(EncryptUtil.encrypt("admin123."));
        rdbDO.setDescription("junit测试");
        rdbDO.addProject(projectDO);

        return rdbRepository.save(rdbDO);
    }
}
