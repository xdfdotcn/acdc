package cn.xdf.acdc.devops.service.process.datasystem.rdb.impl;

import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import cn.xdf.acdc.devops.repository.RdbRepository;
import cn.xdf.acdc.devops.service.entity.RdbDatabaseService;
import cn.xdf.acdc.devops.service.entity.RdbTableService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.MysqlHelperService;
import org.assertj.core.util.Lists;
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
import java.util.Optional;

@RunWith(SpringRunner.class)
@SpringBootTest
public class RdbTablesRefreshTest {

    @MockBean
    private RdbRepository rdbRepository;

    @MockBean
    private RdbDatabaseService rdbDatabaseService;

    @MockBean
    private RdbTableService rdbTableService;

    @MockBean
    private MysqlHelperService mysqlHelperService;

    @Autowired
    private DataSystemMetadataService<RdbDatabaseDO> rdbDatabaseDODataSystemMetadataService;

    @Test
    public void testRefreshMetadataShouldSaveNewTables() {
        Mockito.when(rdbDatabaseService.queryAll(ArgumentMatchers.any())).thenReturn(fakeRdbDatabases());
        Mockito.when(rdbRepository.findById(ArgumentMatchers.eq(fakeRdb().getId()))).thenReturn(Optional.of(fakeRdb()));
        Mockito.when(
                mysqlHelperService.showTables(ArgumentMatchers.eq(fakeRdb()), ArgumentMatchers.eq("db_11"))
        ).thenReturn(fakeTableNames());
        Mockito.when(rdbTableService.queryAll(ArgumentMatchers.any())).thenReturn(fakeOldRdbTables());

        rdbDatabaseDODataSystemMetadataService.refreshMetadata();

        ArgumentCaptor<List<RdbTableDO>> saveTablesCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(rdbTableService).saveAll(saveTablesCaptor.capture());
        List<RdbTableDO> saveTables = saveTablesCaptor.getValue();
        Assert.assertEquals("table_112", saveTables.get(0).getName());
        Assert.assertEquals((Long) 11L, saveTables.get(0).getRdbDatabase().getId());
        Assert.assertEquals("table_113", saveTables.get(1).getName());
        Assert.assertEquals((Long) 11L, saveTables.get(0).getRdbDatabase().getId());
    }

    private List<RdbTableDO> fakeOldRdbTables() {
        List<RdbTableDO> result = new ArrayList<>();
        result.add(RdbTableDO.builder().id(111L).name("table_110").build());
        result.add(RdbTableDO.builder().id(111L).name("table_111").build());
        result.add(RdbTableDO.builder().id(111L).name("table_112_old").build());
        return result;
    }

    private List<String> fakeTableNames() {
        return Lists.newArrayList("table_111", "table_112", "table_113");
    }

    private RdbDO fakeRdb() {
        return RdbDO.builder().id(1L).build();
    }

    private List<RdbDatabaseDO> fakeRdbDatabases() {
        List<RdbDatabaseDO> result = new ArrayList<>();
        result.add(RdbDatabaseDO.builder().id(11L).name("db_11").rdb(fakeRdb()).build());
        return result;
    }
}
