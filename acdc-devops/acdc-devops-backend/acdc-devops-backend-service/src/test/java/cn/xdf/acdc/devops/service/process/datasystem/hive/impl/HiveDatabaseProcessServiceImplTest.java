package cn.xdf.acdc.devops.service.process.datasystem.hive.impl;

import cn.xdf.acdc.devops.core.domain.dto.HiveDbMetaDTO;
import cn.xdf.acdc.devops.core.domain.entity.HdfsDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveTableDO;
import cn.xdf.acdc.devops.core.domain.query.HiveDatabaseQuery;
import cn.xdf.acdc.devops.core.domain.query.HiveTableQuery;
import cn.xdf.acdc.devops.repository.HdfsRepository;
import cn.xdf.acdc.devops.repository.HiveDatabaseRepository;
import cn.xdf.acdc.devops.repository.HiveRepository;
import cn.xdf.acdc.devops.repository.HiveTableRepository;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.HiveHelperService;
import com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest
//@Transactional
// TODO 1.因单元测试中增加事务注解,导致带有事务注解的service级联查询失败
public class HiveDatabaseProcessServiceImplTest {

    @Mock
    private HiveHelperService hiveHelperService;

    @Autowired
    private HiveDatabaseProcessServiceImpl hiveDatabaseProcessServiceImpl;

    @Autowired
    private HiveRepository hiveRepository;

    @Autowired
    private HdfsRepository hdfsRepository;

    @Autowired
    private HiveDatabaseRepository hiveDatabaseRepository;

    @Autowired
    private HiveTableRepository hiveTableRepository;

    private HiveDO hive;

    @Before
    public void setup() {
        ReflectionTestUtils.setField(hiveDatabaseProcessServiceImpl, "hiveHelperService", hiveHelperService);
        initData();
    }

    /**
     * 1. hive元数据为空,正常执行,不进行任何操作
     */
    @Test
    public void testRefreshMetadataShouldDoNothingWhenHiveMetaIsEmpty() {
        when(hiveHelperService.fetchHiveDbMeta()).thenReturn(Collections.EMPTY_LIST);
        hiveDatabaseProcessServiceImpl.refreshMetadata();

        Long dbDatabaseCount = hiveDatabaseRepository.query(new HiveDatabaseQuery()).stream().count();
        Long dbTableCount = hiveTableRepository.queryAll(new HiveTableQuery()).stream().count();
        Assertions.assertThat(dbDatabaseCount).isEqualTo(0);
        Assertions.assertThat(dbTableCount).isEqualTo(0);
    }

    /**
     * 2. DB不存在库和表,插入所有的库和表(发生在第一次同步的情况)
     */
    @Test
    public void testRefreshMetadataShouldSuccessWhenDbEmptyAndHiveMetaNotEmpty() {
        List<HiveDbMetaDTO> metas = Lists.newArrayList(
                HiveDbMetaDTO.builder().db("db1").table("tb1").build(),
                HiveDbMetaDTO.builder().db("db1").table("tb2").build(),
                HiveDbMetaDTO.builder().db("db2").table("tb3").build()
        );

        when(hiveHelperService.fetchHiveDbMeta()).thenReturn(metas);
        hiveDatabaseProcessServiceImpl.refreshMetadata();
        Map<String, HiveDatabaseDO> hiveDatabaseMap = hiveDatabaseRepository.query(new HiveDatabaseQuery())
                .stream().collect(Collectors.toMap(HiveDatabaseDO::getName, it -> it));

        Map<String, HiveTableDO> hiveTableMap = hiveTableRepository.queryAll(new HiveTableQuery())
                .stream().collect(Collectors.toMap(HiveTableDO::getName, it -> it));

        Assertions.assertThat(hiveTableMap.get("tb1").getName()).isEqualTo("tb1");
        Assertions.assertThat(hiveTableMap.get("tb2").getName()).isEqualTo("tb2");
        Assertions.assertThat(hiveTableMap.get("tb3").getName()).isEqualTo("tb3");

        Assertions.assertThat(hiveTableMap.get("tb1").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db1").getId());
        Assertions.assertThat(hiveTableMap.get("tb2").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db1").getId());
        Assertions.assertThat(hiveTableMap.get("tb3").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db2").getId());
    }

    /**
     * 3. DB 与hiveMeta 数据相同,不做任何处理
     */
    @Test
    public void testRefreshMetadataShouldDoNothingWhenHiveMetaEqualDbData() {
        HiveDatabaseDO savedHiveDatabase1 = hiveDatabaseRepository.save(new HiveDatabaseDO().setName("db1").setHive(hive));
        hiveTableRepository.save(HiveTableDO.builder().name("db1_tb1").hiveDatabase(savedHiveDatabase1).build());
        hiveTableRepository.save(HiveTableDO.builder().name("db1_tb2").hiveDatabase(savedHiveDatabase1).build());

        HiveDatabaseDO savedHiveDatabase2 = hiveDatabaseRepository.save(new HiveDatabaseDO().setName("db2").setHive(hive));
        hiveTableRepository.save(HiveTableDO.builder().name("db2_tb1").hiveDatabase(savedHiveDatabase2).build());
        hiveTableRepository.save(HiveTableDO.builder().name("db2_tb2").hiveDatabase(savedHiveDatabase2).build());

        List<HiveDbMetaDTO> metas = Lists.newArrayList(
                HiveDbMetaDTO.builder().db("db1").table("db1_tb1").build(),
                HiveDbMetaDTO.builder().db("db1").table("db1_tb2").build(),

                HiveDbMetaDTO.builder().db("db2").table("db2_tb1").build(),
                HiveDbMetaDTO.builder().db("db2").table("db2_tb2").build()
        );

        when(hiveHelperService.fetchHiveDbMeta()).thenReturn(metas);
        hiveDatabaseProcessServiceImpl.refreshMetadata();
        Map<String, HiveDatabaseDO> hiveDatabaseMap = hiveDatabaseRepository.query(new HiveDatabaseQuery())
                .stream().collect(Collectors.toMap(HiveDatabaseDO::getName, it -> it));

        Map<String, HiveTableDO> hiveTableMap = hiveTableRepository.queryAll(new HiveTableQuery())
                .stream().collect(Collectors.toMap(HiveTableDO::getName, it -> it));

        Assertions.assertThat(hiveTableMap.get("db1_tb1").getName()).isEqualTo("db1_tb1");
        Assertions.assertThat(hiveTableMap.get("db1_tb2").getName()).isEqualTo("db1_tb2");

        Assertions.assertThat(hiveTableMap.get("db2_tb1").getName()).isEqualTo("db2_tb1");
        Assertions.assertThat(hiveTableMap.get("db2_tb2").getName()).isEqualTo("db2_tb2");

        Assertions.assertThat(hiveTableMap.get("db1_tb1").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db1").getId());
        Assertions.assertThat(hiveTableMap.get("db1_tb2").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db1").getId());

        Assertions.assertThat(hiveTableMap.get("db2_tb1").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db2").getId());
        Assertions.assertThat(hiveTableMap.get("db2_tb2").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db2").getId());
    }

    /**
     * 4. DB存在库和表,hive元数据增加"新库"情况
     */
    @Test
    public void testRefreshMetadataShouldAddDatabaseWhenHiveMetaAddNewDatabase() {
        HiveDatabaseDO savedHiveDatabase1 = hiveDatabaseRepository.save(new HiveDatabaseDO().setName("db1").setHive(hive));
        hiveTableRepository.save(HiveTableDO.builder().name("db1_tb1").hiveDatabase(savedHiveDatabase1).build());
        hiveTableRepository.save(HiveTableDO.builder().name("db1_tb2").hiveDatabase(savedHiveDatabase1).build());

        HiveDatabaseDO savedHiveDatabase2 = hiveDatabaseRepository.save(new HiveDatabaseDO().setName("db2").setHive(hive));
        hiveTableRepository.save(HiveTableDO.builder().name("db2_tb1").hiveDatabase(savedHiveDatabase2).build());
        hiveTableRepository.save(HiveTableDO.builder().name("db2_tb2").hiveDatabase(savedHiveDatabase2).build());

        List<HiveDbMetaDTO> metas = Lists.newArrayList(
                HiveDbMetaDTO.builder().db("db1").table("db1_tb1").build(),
                HiveDbMetaDTO.builder().db("db1").table("db1_tb2").build(),

                HiveDbMetaDTO.builder().db("db2").table("db2_tb1").build(),
                HiveDbMetaDTO.builder().db("db2").table("db2_tb2").build(),

                HiveDbMetaDTO.builder().db("db3").table("db3_tb1").build(),
                HiveDbMetaDTO.builder().db("db3").table("db3_tb2").build()
        );

        when(hiveHelperService.fetchHiveDbMeta()).thenReturn(metas);
        hiveDatabaseProcessServiceImpl.refreshMetadata();
        Map<String, HiveDatabaseDO> hiveDatabaseMap = hiveDatabaseRepository.query(new HiveDatabaseQuery())
                .stream().collect(Collectors.toMap(HiveDatabaseDO::getName, it -> it));

        Map<String, HiveTableDO> hiveTableMap = hiveTableRepository.queryAll(new HiveTableQuery())
                .stream().collect(Collectors.toMap(HiveTableDO::getName, it -> it));

        Assertions.assertThat(hiveTableMap.get("db1_tb1").getName()).isEqualTo("db1_tb1");
        Assertions.assertThat(hiveTableMap.get("db1_tb2").getName()).isEqualTo("db1_tb2");

        Assertions.assertThat(hiveTableMap.get("db2_tb1").getName()).isEqualTo("db2_tb1");
        Assertions.assertThat(hiveTableMap.get("db2_tb2").getName()).isEqualTo("db2_tb2");

        Assertions.assertThat(hiveTableMap.get("db3_tb1").getName()).isEqualTo("db3_tb1");
        Assertions.assertThat(hiveTableMap.get("db3_tb2").getName()).isEqualTo("db3_tb2");

        Assertions.assertThat(hiveTableMap.get("db1_tb1").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db1").getId());
        Assertions.assertThat(hiveTableMap.get("db1_tb2").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db1").getId());

        Assertions.assertThat(hiveTableMap.get("db2_tb1").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db2").getId());
        Assertions.assertThat(hiveTableMap.get("db2_tb2").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db2").getId());

        Assertions.assertThat(hiveTableMap.get("db3_tb1").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db3").getId());
        Assertions.assertThat(hiveTableMap.get("db3_tb2").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db3").getId());
    }

    /**
     * 5. DB存在库和表,hive元数据增加"新库"和"原库增加表"的情况
     */
    @Test
    public void testRefreshMetadataShouldAddDatabaseAndTableWhenHiveMetaAddNewDatabaseAndTable() {
        HiveDatabaseDO savedHiveDatabase1 = hiveDatabaseRepository.save(new HiveDatabaseDO().setName("db1").setHive(hive));
        hiveTableRepository.save(HiveTableDO.builder().name("db1_tb1").hiveDatabase(savedHiveDatabase1).build());
        hiveTableRepository.save(HiveTableDO.builder().name("db1_tb2").hiveDatabase(savedHiveDatabase1).build());

        HiveDatabaseDO savedHiveDatabase2 = hiveDatabaseRepository.save(new HiveDatabaseDO().setName("db2").setHive(hive));
        hiveTableRepository.save(HiveTableDO.builder().name("db2_tb1").hiveDatabase(savedHiveDatabase2).build());
        hiveTableRepository.save(HiveTableDO.builder().name("db2_tb2").hiveDatabase(savedHiveDatabase2).build());

        List<HiveDbMetaDTO> metas = Lists.newArrayList(
                HiveDbMetaDTO.builder().db("db1").table("db1_tb1").build(),
                HiveDbMetaDTO.builder().db("db1").table("db1_tb2").build(),
                HiveDbMetaDTO.builder().db("db1").table("db1_tb3").build(),
                HiveDbMetaDTO.builder().db("db1").table("db1_tb4").build(),

                HiveDbMetaDTO.builder().db("db2").table("db2_tb1").build(),
                HiveDbMetaDTO.builder().db("db2").table("db2_tb2").build(),
                HiveDbMetaDTO.builder().db("db2").table("db2_tb3").build(),
                HiveDbMetaDTO.builder().db("db2").table("db2_tb4").build(),

                HiveDbMetaDTO.builder().db("db3").table("db3_tb1").build(),
                HiveDbMetaDTO.builder().db("db3").table("db3_tb2").build()
        );

        when(hiveHelperService.fetchHiveDbMeta()).thenReturn(metas);
        hiveDatabaseProcessServiceImpl.refreshMetadata();
        Map<String, HiveDatabaseDO> hiveDatabaseMap = hiveDatabaseRepository.query(new HiveDatabaseQuery())
                .stream().collect(Collectors.toMap(HiveDatabaseDO::getName, it -> it));

        Map<String, HiveTableDO> hiveTableMap = hiveTableRepository.queryAll(new HiveTableQuery())
                .stream().collect(Collectors.toMap(HiveTableDO::getName, it -> it));

        Assertions.assertThat(hiveTableMap.get("db1_tb1").getName()).isEqualTo("db1_tb1");
        Assertions.assertThat(hiveTableMap.get("db1_tb2").getName()).isEqualTo("db1_tb2");
        Assertions.assertThat(hiveTableMap.get("db1_tb3").getName()).isEqualTo("db1_tb3");
        Assertions.assertThat(hiveTableMap.get("db1_tb4").getName()).isEqualTo("db1_tb4");

        Assertions.assertThat(hiveTableMap.get("db2_tb1").getName()).isEqualTo("db2_tb1");
        Assertions.assertThat(hiveTableMap.get("db2_tb2").getName()).isEqualTo("db2_tb2");
        Assertions.assertThat(hiveTableMap.get("db2_tb3").getName()).isEqualTo("db2_tb3");
        Assertions.assertThat(hiveTableMap.get("db2_tb4").getName()).isEqualTo("db2_tb4");

        Assertions.assertThat(hiveTableMap.get("db3_tb1").getName()).isEqualTo("db3_tb1");
        Assertions.assertThat(hiveTableMap.get("db3_tb2").getName()).isEqualTo("db3_tb2");

        Assertions.assertThat(hiveTableMap.get("db1_tb1").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db1").getId());
        Assertions.assertThat(hiveTableMap.get("db1_tb2").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db1").getId());
        Assertions.assertThat(hiveTableMap.get("db1_tb3").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db1").getId());
        Assertions.assertThat(hiveTableMap.get("db1_tb4").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db1").getId());

        Assertions.assertThat(hiveTableMap.get("db2_tb1").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db2").getId());
        Assertions.assertThat(hiveTableMap.get("db2_tb2").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db2").getId());
        Assertions.assertThat(hiveTableMap.get("db2_tb3").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db2").getId());
        Assertions.assertThat(hiveTableMap.get("db2_tb4").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db2").getId());

        Assertions.assertThat(hiveTableMap.get("db3_tb1").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db3").getId());
        Assertions.assertThat(hiveTableMap.get("db3_tb2").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db3").getId());
    }

    /**
     * 6. DB存在库和表,hiveMeta 删除"库"的情况
     */
    @Test
    public void testRefreshMetadataShouldDelHiveDatabaseWhenHiveMetaDelHiveDatabase() {
        HiveDatabaseDO savedHiveDatabase1 = hiveDatabaseRepository.save(new HiveDatabaseDO().setName("db1").setHive(hive));
        hiveTableRepository.save(HiveTableDO.builder().name("db1_tb1").hiveDatabase(savedHiveDatabase1).build());
        hiveTableRepository.save(HiveTableDO.builder().name("db1_tb2").hiveDatabase(savedHiveDatabase1).build());

        HiveDatabaseDO savedHiveDatabase2 = hiveDatabaseRepository.save(new HiveDatabaseDO().setName("db2").setHive(hive));
        hiveTableRepository.save(HiveTableDO.builder().name("db2_tb1").hiveDatabase(savedHiveDatabase2).build());
        hiveTableRepository.save(HiveTableDO.builder().name("db2_tb2").hiveDatabase(savedHiveDatabase2).build());

        HiveDatabaseDO savedHiveDatabase3 = hiveDatabaseRepository.save(new HiveDatabaseDO().setName("db3").setHive(hive));
        hiveTableRepository.save(HiveTableDO.builder().name("db3_tb1").hiveDatabase(savedHiveDatabase3).build());
        hiveTableRepository.save(HiveTableDO.builder().name("db3_tb2").hiveDatabase(savedHiveDatabase3).build());

        List<HiveDbMetaDTO> metas = Lists.newArrayList(
                HiveDbMetaDTO.builder().db("db1").table("db1_tb1").build(),
                HiveDbMetaDTO.builder().db("db1").table("db1_tb2").build()
        );

        when(hiveHelperService.fetchHiveDbMeta()).thenReturn(metas);
        hiveDatabaseProcessServiceImpl.refreshMetadata();
        Map<String, HiveDatabaseDO> hiveDatabaseMap = hiveDatabaseRepository.query(new HiveDatabaseQuery())
                .stream().collect(Collectors.toMap(HiveDatabaseDO::getName, it -> it));

        Map<String, HiveTableDO> hiveTableMap = hiveTableRepository.queryAll(new HiveTableQuery())
                .stream().collect(Collectors.toMap(HiveTableDO::getName, it -> it));

        Assertions.assertThat(hiveTableMap.get("db1_tb1").getName()).isEqualTo("db1_tb1");
        Assertions.assertThat(hiveTableMap.get("db1_tb2").getName()).isEqualTo("db1_tb2");

        Assertions.assertThat(hiveTableMap.get("db3_tb1")).isEqualTo(null);
        Assertions.assertThat(hiveTableMap.get("db3_tb2")).isEqualTo(null);

        Assertions.assertThat(hiveTableMap.get("db2_tb1")).isEqualTo(null);
        Assertions.assertThat(hiveTableMap.get("db2_tb2")).isEqualTo(null);

        Assertions.assertThat(hiveDatabaseMap.get("db3")).isEqualTo(null);
        Assertions.assertThat(hiveDatabaseMap.get("db2")).isEqualTo(null);
    }

    /**
     * 7. DB存在库和表,hiveMeta 删除"库"和删除已存在库中的表
     */
    @Test
    public void testRefreshMetadataShouldDelHiveDatabaseAndTableWhenHiveMetaDelHiveDatabaseAndTable() {
        HiveDatabaseDO savedHiveDatabase1 = hiveDatabaseRepository.save(new HiveDatabaseDO().setName("db1").setHive(hive));
        hiveTableRepository.save(HiveTableDO.builder().name("db1_tb1").hiveDatabase(savedHiveDatabase1).build());
        hiveTableRepository.save(HiveTableDO.builder().name("db1_tb2").hiveDatabase(savedHiveDatabase1).build());

        HiveDatabaseDO savedHiveDatabase2 = hiveDatabaseRepository.save(new HiveDatabaseDO().setName("db2").setHive(hive));
        hiveTableRepository.save(HiveTableDO.builder().name("db2_tb1").hiveDatabase(savedHiveDatabase2).build());
        hiveTableRepository.save(HiveTableDO.builder().name("db2_tb2").hiveDatabase(savedHiveDatabase2).build());

        HiveDatabaseDO savedHiveDatabase3 = hiveDatabaseRepository.save(new HiveDatabaseDO().setName("db3").setHive(hive));
        hiveTableRepository.save(HiveTableDO.builder().name("db3_tb1").hiveDatabase(savedHiveDatabase3).build());
        hiveTableRepository.save(HiveTableDO.builder().name("db3_tb2").hiveDatabase(savedHiveDatabase3).build());

        List<HiveDbMetaDTO> metas = Lists.newArrayList(
                HiveDbMetaDTO.builder().db("db1").table("db1_tb1").build()
        );

        when(hiveHelperService.fetchHiveDbMeta()).thenReturn(metas);
        hiveDatabaseProcessServiceImpl.refreshMetadata();
        Map<String, HiveDatabaseDO> hiveDatabaseMap = hiveDatabaseRepository.query(new HiveDatabaseQuery())
                .stream().collect(Collectors.toMap(HiveDatabaseDO::getName, it -> it));

        Map<String, HiveTableDO> hiveTableMap = hiveTableRepository.queryAll(new HiveTableQuery())
                .stream().collect(Collectors.toMap(HiveTableDO::getName, it -> it));

        Assertions.assertThat(hiveTableMap.get("db1_tb1").getName()).isEqualTo("db1_tb1");

        Assertions.assertThat(hiveTableMap.get("db1_tb2")).isEqualTo(null);

        Assertions.assertThat(hiveTableMap.get("db2_tb1")).isEqualTo(null);
        Assertions.assertThat(hiveTableMap.get("db2_tb2")).isEqualTo(null);

        Assertions.assertThat(hiveTableMap.get("db3_tb1")).isEqualTo(null);
        Assertions.assertThat(hiveTableMap.get("db3_tb2")).isEqualTo(null);

        Assertions.assertThat(hiveDatabaseMap.get("db3")).isEqualTo(null);
        Assertions.assertThat(hiveDatabaseMap.get("db2")).isEqualTo(null);
    }

    /**
     * 8. 增加整库,增加已存在库的表,删除整库,删除已经存在库的表
     */
    @Test
    public void testRefreshMetadata() {
        HiveDatabaseDO savedHiveDatabase1 = hiveDatabaseRepository.save(new HiveDatabaseDO().setName("db1").setHive(hive));
        hiveTableRepository.save(HiveTableDO.builder().name("db1_tb1").hiveDatabase(savedHiveDatabase1).build());
        hiveTableRepository.save(HiveTableDO.builder().name("db1_tb2").hiveDatabase(savedHiveDatabase1).build());

        HiveDatabaseDO savedHiveDatabase2 = hiveDatabaseRepository.save(new HiveDatabaseDO().setName("db2").setHive(hive));
        hiveTableRepository.save(HiveTableDO.builder().name("db2_tb1").hiveDatabase(savedHiveDatabase2).build());
        hiveTableRepository.save(HiveTableDO.builder().name("db2_tb2").hiveDatabase(savedHiveDatabase2).build());

        HiveDatabaseDO savedHiveDatabase3 = hiveDatabaseRepository.save(new HiveDatabaseDO().setName("db3").setHive(hive));
        hiveTableRepository.save(HiveTableDO.builder().name("db3_tb1").hiveDatabase(savedHiveDatabase3).build());
        hiveTableRepository.save(HiveTableDO.builder().name("db3_tb2").hiveDatabase(savedHiveDatabase3).build());

        HiveDatabaseDO savedHiveDatabase4 = hiveDatabaseRepository.save(new HiveDatabaseDO().setName("db4").setHive(hive));
        hiveTableRepository.save(HiveTableDO.builder().name("db4_tb1").hiveDatabase(savedHiveDatabase4).build());
        hiveTableRepository.save(HiveTableDO.builder().name("db4_tb2").hiveDatabase(savedHiveDatabase4).build());

        List<HiveDbMetaDTO> metas = Lists.newArrayList(
                // 1. 删除 db3, db4

                // 2. db1 增加 db1_tb3, 删除 db1_tb1
                HiveDbMetaDTO.builder().db("db1").table("db1_tb2").build(),
                HiveDbMetaDTO.builder().db("db1").table("db1_tb3").build(),

                // 3. db2 增加 db2_tb3, 删除 db2_tb1
                HiveDbMetaDTO.builder().db("db2").table("db2_tb2").build(),
                HiveDbMetaDTO.builder().db("db2").table("db2_tb3").build(),

                // 4. 增加 db5
                HiveDbMetaDTO.builder().db("db5").table("db5_tb1").build(),
                HiveDbMetaDTO.builder().db("db5").table("db5_tb2").build(),

                // 5. 增加 db6
                HiveDbMetaDTO.builder().db("db6").table("db6_tb1").build(),
                HiveDbMetaDTO.builder().db("db6").table("db6_tb2").build()
        );

        when(hiveHelperService.fetchHiveDbMeta()).thenReturn(metas);
        hiveDatabaseProcessServiceImpl.refreshMetadata();
        Map<String, HiveDatabaseDO> hiveDatabaseMap = hiveDatabaseRepository.query(new HiveDatabaseQuery())
                .stream().collect(Collectors.toMap(HiveDatabaseDO::getName, it -> it));

        Map<String, HiveTableDO> hiveTableMap = hiveTableRepository.queryAll(new HiveTableQuery())
                .stream().collect(Collectors.toMap(HiveTableDO::getName, it -> it));

        Assertions.assertThat(hiveDatabaseMap.size()).isEqualTo(4);
        Assertions.assertThat(hiveTableMap.size()).isEqualTo(8);

        Assertions.assertThat(hiveTableMap.get("db1_tb2").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db1").getId());
        Assertions.assertThat(hiveTableMap.get("db1_tb3").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db1").getId());

        Assertions.assertThat(hiveTableMap.get("db2_tb2").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db2").getId());
        Assertions.assertThat(hiveTableMap.get("db2_tb3").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db2").getId());

        Assertions.assertThat(hiveTableMap.get("db5_tb1").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db5").getId());
        Assertions.assertThat(hiveTableMap.get("db5_tb1").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db5").getId());

        Assertions.assertThat(hiveTableMap.get("db6_tb1").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db6").getId());
        Assertions.assertThat(hiveTableMap.get("db6_tb1").getHiveDatabase().getId()).isEqualTo(hiveDatabaseMap.get("db6").getId());
    }

    private void initData() {
        HdfsDO hdfs = hdfsRepository.save(HdfsDO.builder()
                .id(1L)
                .name("hdfs")
                .clientFailoverProxyProvider("test")
                .build());
        hive = hiveRepository.save(HiveDO.builder()
                .id(1L)
                .name("hive")
                .metastoreUris("test://test")
                .hdfsUser("hive")
                .hdfs(hdfs)
                .build());
    }
}
