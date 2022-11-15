package cn.xdf.acdc.devops.service.process.datasystem.rdb.impl;

import cn.xdf.acdc.devops.core.domain.dto.RdbInstanceDTO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ProjectSourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RoleType;
import cn.xdf.acdc.devops.repository.ProjectRepository;
import cn.xdf.acdc.devops.repository.RdbDatabaseRepository;
import cn.xdf.acdc.devops.repository.RdbInstanceRepository;
import cn.xdf.acdc.devops.repository.RdbRepository;
import cn.xdf.acdc.devops.repository.RdbTableRepository;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.RdbInstanceProcessService;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.MysqlHelperService;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

// CHECKSTYLE:OFF

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class RdbInstanceProcessServiceImplTest {

    @Autowired
    private ProjectRepository projectRepository;

    @Autowired
    private RdbRepository rdbRepository;

    @Autowired
    private RdbInstanceRepository rdbInstanceRepository;

    @Autowired
    private RdbDatabaseRepository rdbDatabaseRepository;

    @Autowired
    private RdbTableRepository rdbTableRepository;

    @Autowired
    private RdbInstanceProcessService rdbInstanceProcessService;

    @MockBean
    private MysqlHelperService mysqlHelperService;

    @Before
    public void setup() {
        // mock show databases, show tables
        when(mysqlHelperService.showDataBases(any(), any())).thenReturn(Lists.newArrayList("database_1", "database_2", "database_3"));
        when(mysqlHelperService.showTables(any(), anyString())).thenReturn(Lists.newArrayList("table_1", "table_2", "table_3"));

        mockRdbInstance();
    }

    @Test
    public void testGetRdbInstance() {
        RdbInstanceDTO rdbInstance = rdbInstanceProcessService.getRdbInstance(1L);
        Assertions.assertThat(rdbInstance.getHost()).isEqualTo("192.168.1.1");
    }

    @Test
    public void testQueryInstancesByRdb() {
        List<RdbInstanceDTO> instanceDTOS = rdbInstanceProcessService.queryInstancesByRdbId(1L);
        Assertions.assertThat(instanceDTOS.size()).isEqualTo(2);
    }

    @Test
    public void testSaveRdbInstances() {
        RdbInstanceDTO dto = RdbInstanceDTO.builder().id(3L).host("mysql://host")
                .port("3306").roleType(RoleType.MASTER).build();
        rdbInstanceProcessService.saveRdbInstances(1L, Lists.newArrayList(dto));

        Optional<RdbDO> rdbDO = rdbRepository.findById(1L);
        Assertions.assertThat(rdbDO.isPresent()).isEqualTo(true);

        // assert database
        List<RdbDatabaseDO> databases = rdbDatabaseRepository.findAllByRdb(rdbDO.get());
        Assertions.assertThat(databases.size()).isEqualTo(3);

        // assert table in each database
        for (RdbDatabaseDO each : databases) {
            List<RdbTableDO> rdbTableDOs = rdbTableRepository.findAllByRdbDatabaseId(each.getId());
            Assertions.assertThat(rdbTableDOs.size()).isEqualTo(3);
        }
    }

    private void mockRdbInstance() {
        ProjectDO projectDO = new ProjectDO();
        projectDO.setId(1L);
        projectDO.setName("测试项目");
        projectDO.setDescription("Test");
        projectDO.setSource(ProjectSourceType.USER_INPUT);
        projectDO.setOriginalId(1L);
        projectRepository.save(projectDO);

        RdbDO rdbDO = new RdbDO();
        rdbDO.setId(1L);
        rdbDO.setName("测试");
        rdbDO.setRdbType("mysql");
        rdbDO.setUsername("root");
        rdbDO.setPassword(EncryptUtil.encrypt("irDIaBmO3RhT"));
        rdbDO.setUpdateTime(Instant.now());
        rdbDO.setCreationTime(Instant.now());
        rdbDO.setDescription("junit测试");
        rdbDO.setProjects(Sets.newHashSet(projectDO));
        rdbRepository.save(rdbDO);

        RdbInstanceDO instance1 = new RdbInstanceDO();
        instance1.setRdb(rdbDO);
        instance1.setRole(RoleType.MASTER);
        instance1.setHost("192.168.1.1");
        instance1.setPort(3306);
        instance1.setId(1L);
        instance1.setUpdateTime(Instant.now());
        instance1.setCreationTime(Instant.now());

        RdbInstanceDO instance2 = new RdbInstanceDO();
        instance2.setRdb(rdbDO);
        instance2.setRole(RoleType.DATA_SOURCE);
        instance2.setHost("192.168.1.2");
        instance2.setPort(3306);
        instance2.setId(3L);
        instance2.setUpdateTime(Instant.now());
        instance2.setCreationTime(Instant.now());

        rdbInstanceRepository.saveAll(Sets.newHashSet(instance1, instance2));
    }
}
