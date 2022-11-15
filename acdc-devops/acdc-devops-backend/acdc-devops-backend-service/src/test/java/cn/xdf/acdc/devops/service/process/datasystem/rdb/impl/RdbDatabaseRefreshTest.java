package cn.xdf.acdc.devops.service.process.datasystem.rdb.impl;

import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.repository.RdbRepository;
import cn.xdf.acdc.devops.service.entity.RdbDatabaseService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.MysqlHelperService;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class RdbDatabaseRefreshTest {

    @MockBean
    private RdbRepository rdbRepository;

    @MockBean
    private MysqlHelperService mysqlHelperService;

    @MockBean
    private RdbDatabaseService rdbDatabaseService;

    @Autowired
    private DataSystemMetadataService<RdbDO> rdbDataSystemMetadataService;

    @Test
    public void testRefreshMetadataShouldSaveNewDatabases() {
        Mockito.when(rdbRepository.findAll()).thenReturn(fakeRdbs());
        Mockito.when(mysqlHelperService.showDataBases(ArgumentMatchers.any(RdbDO.class), ArgumentMatchers.any()))
                .thenReturn(fakeDbNames());
        Mockito.when(rdbDatabaseService.queryAll(ArgumentMatchers.any())).thenReturn(fakeRdbDatabases());
        rdbDataSystemMetadataService.refreshMetadata();

        ArgumentCaptor<List<RdbDatabaseDO>> rdbDatabaseCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(rdbDatabaseService).saveAll(rdbDatabaseCaptor.capture());
        List<RdbDatabaseDO> result = rdbDatabaseCaptor.getValue();
        Assert.assertEquals("db_12", result.get(0).getName());
        Assert.assertEquals("db_13", result.get(1).getName());
    }

    private List<RdbDatabaseDO> fakeRdbDatabases() {
        List<RdbDatabaseDO> result = new ArrayList<>();
        result.add(RdbDatabaseDO.builder().id(10L).name("db_10").rdb(fakeRdbs().get(0)).build());
        result.add(RdbDatabaseDO.builder().id(11L).name("db_11").rdb(fakeRdbs().get(0)).build());
        result.add(RdbDatabaseDO.builder().id(13L).name("db_12_old").rdb(fakeRdbs().get(0)).build());
        return result;
    }

    private List<String> fakeDbNames() {
        return Lists.newArrayList("db_11", "db_12", "db_13");
    }

    private List<RdbDO> fakeRdbs() {
        List<RdbDO> result = new ArrayList<>();
        RdbDO rdbDO = new RdbDO();
        rdbDO.setId(1L);
        result.add(rdbDO);
        return result;
    }
}
