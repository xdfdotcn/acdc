package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.HiveTableDO;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class HiveTableServiceT {

    @Autowired
    private HiveTableService hiveTableService;

    @Before
    public void setUp() {
    }

    @Test
    public void testSaveShouldInsertWhenNotExist() {
        HiveTableDO hiveTableDO = HiveTableDO.builder().name("hive_table_name").build();
        hiveTableService.save(hiveTableDO);

        Assertions.assertThat(hiveTableDO.getId()).isNotNull();
    }

    @Test
    public void testSaveDeletedShouldEqualsFalseWhenDefault() {
        HiveTableDO hiveTableDO = HiveTableDO.builder().name("hive_table_name").build();
        hiveTableService.save(hiveTableDO);

        Assertions.assertThat(hiveTableDO.getDeleted()).isFalse();
    }

    @Test
    public void testSaveCreationTimeShouldNotNull() {
        HiveTableDO hiveTableDO = HiveTableDO.builder().name("hive_table_name").build();
        hiveTableService.save(hiveTableDO);

        Assertions.assertThat(hiveTableDO.getCreationTime()).isNotNull();
    }
}
