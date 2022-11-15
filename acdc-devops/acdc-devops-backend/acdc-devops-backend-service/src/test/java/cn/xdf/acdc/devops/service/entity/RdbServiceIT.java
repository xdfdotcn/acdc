package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.query.RdbQuery;
import cn.xdf.acdc.devops.repository.RdbRepository;
import com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class RdbServiceIT {

    @Autowired
    private RdbService rdbService;

    @Autowired
    private RdbRepository rdbRepository;

    @Autowired
    private KafkaClusterService kafkaClusterService;

    @Test
    public void testSaveShouldInsertWhenNotExist() {
        RdbDO rdb = new RdbDO();
        rdb.setRdbType("mysql");
        rdb.setName("a cdc-cluster");
        rdb.setUsername("root");
        rdb.setPassword("123456");
        rdb.setDescription("desc");
        rdb.setCreationTime(new Date().toInstant());
        rdb.setUpdateTime(new Date().toInstant());
        RdbDO saveResult = rdbService.save(rdb);

        Assertions.assertThat(saveResult.getId()).isNotNull();
    }

    @Test
    public void testSaveShouldInsertWhenUseBuilderAndNotExist() {
        RdbDO rdb = RdbDO.builder()
                .rdbType("mysql")
                .name("acdc cluster")
                .username("fake-user")
                .password("fake-password")
                .description("desc")
                .build();

        RdbDO saveResult = rdbService.save(rdb);
        rdb.setId(saveResult.getId());
        Assertions.assertThat(saveResult).isEqualTo(rdb);
    }

    @Test
    public void testSaveShouldUpdateWhenExist() {
        RdbDO rdb = new RdbDO();
        rdb.setId(1L);
        rdb.setRdbType("mysql");
        rdb.setName("a cdc-cluster");
        rdb.setUsername("root");
        rdb.setPassword("123456");
        rdb.setDescription("desc");
        rdb.setCreationTime(new Date().toInstant());
        rdb.setUpdateTime(new Date().toInstant());
        RdbDO saveResult1 = rdbService.save(rdb);
        saveResult1.setName("test2");
        RdbDO saveResult2 = rdbService.save(saveResult1);
        Assertions.assertThat(saveResult2.getName()).isEqualTo("test2");
        Assertions.assertThat(rdbRepository.queryAll(new RdbQuery()).size()).isEqualTo(1);
    }

    @Test
    public void testSaveAllShouldInsertWhenNotExist() {
        List<RdbDO> rdbList = createRdbList();
        List<RdbDO> saveResultList = rdbService.saveAll(rdbList);

        Assertions.assertThat(saveResultList.size()).isEqualTo(rdbList.size());

        for (int i = 0; i < rdbList.size(); i++) {
            rdbList.get(i).setId(saveResultList.get(i).getId());
            Assertions.assertThat(saveResultList.get(i)).isEqualTo(rdbList.get(i));
        }
    }

    @Test
    public void testSaveAllShouldUpdateWhenExist() {
        List<RdbDO> rdbList = createRdbList();
        List<RdbDO> saveResultList = rdbService.saveAll(rdbList);
        saveResultList.forEach(rdb -> rdb.setDescription("test_update"));
        rdbService.saveAll(saveResultList).forEach(rdb -> {
            Assertions.assertThat(rdb.getDescription()).isEqualTo("test_update");
        });
        Assertions.assertThat(rdbService.saveAll(saveResultList).size()).isEqualTo(rdbList.size());
    }

    @Test
    public void testSaveAllShouldFailWhenMissingNotNullField() {
        List<RdbDO> rdbList = createRdbList();
        rdbList.forEach(rdb -> rdb.setName(null));
        rdbService.saveAll(rdbList);
    }

    @Test(expected = InvalidDataAccessApiUsageException.class)
    public void testSaveAllShouldFailWhenGivenNull() {
        rdbService.saveAll(null);
    }

    @Test
    public void testQueryAllShouldSuccess() {
        List<RdbDO> rdbList = createRdbListByCount(20);
        rdbService.saveAll(rdbList);

        List<RdbDO> queryResultList = rdbRepository.queryAll(RdbQuery.builder().name("rdb1").build());
        Assertions.assertThat(queryResultList.size()).isEqualTo(10);
        queryResultList.forEach(r -> Assertions.assertThat(r.getName()).contains("rdb1"));

        queryResultList = rdbRepository.queryAll(new RdbQuery());
        long p1Count = queryResultList.stream().filter(item -> item.getName().contains("rdb1")).count();
        long p2Count = queryResultList.stream().filter(item -> item.getName().contains("rdb2")).count();
        Assertions.assertThat(queryResultList.size()).isEqualTo(20);
        Assertions.assertThat(p1Count).isEqualTo(10);
        Assertions.assertThat(p2Count).isEqualTo(10);
    }

    @Test(expected = NullPointerException.class)
    public void testQueryAllShouldFailWhenGiveNull() {
        rdbRepository.queryAll(null);
    }

    @Test
    public void testQuery() {
        List<RdbDO> rdbList = createRdbList();
        rdbService.saveAll(rdbList);

        // 分页正常滚动
        RdbQuery rdbQuery = RdbQuery.builder().name("cdc").build();

        Page<RdbDO> page = rdbRepository.queryAll(rdbQuery, createPageRequest(1, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = rdbRepository.queryAll(rdbQuery, createPageRequest(2, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = rdbRepository.queryAll(rdbQuery, createPageRequest(3, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = rdbRepository.queryAll(rdbQuery, createPageRequest(4, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(1);

        // 过滤条件不存在
        rdbQuery.setName("kk-not-exist");
        page = rdbRepository.queryAll(rdbQuery, createPageRequest(1, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(0);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(0);
        Assertions.assertThat(page.getContent().size()).isEqualTo(0);

        // 更改pageSize,取消传入查询条件
        page = rdbRepository.queryAll(new RdbQuery(), createPageRequest(1, 10));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(1);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(10);

        // 页越界
        rdbQuery.setName("cdc");
        page = rdbRepository.queryAll(rdbQuery, createPageRequest(999, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueryShouldFailWhenGivenIllegalPageIndex() {
        rdbRepository.queryAll(new RdbQuery(), createPageRequest(-999, -666));
    }

    private Pageable createPageRequest(final int pageIndex, final int pageSize) {
        Pageable pageable = PageRequest.of(pageIndex - 1, pageSize, Sort.by(Sort.Order.desc("name")));
        return pageable;
    }

    private List<RdbDO> createRdbListByCount(int count) {
        Assert.assertTrue(count >= 1);

        List<RdbDO> rdbList = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            String name = i % 2 == 0 ? "rdb1-" + i : "rdb2-" + i;
            RdbDO rdb = new RdbDO();
            rdb.setId(Long.valueOf(i + 1));
            rdb.setRdbType("mysql");
            rdb.setName(name);
            rdb.setUsername("root");
            rdb.setPassword("123456");
            rdb.setDescription("desc");
            rdb.setCreationTime(new Date().toInstant());
            rdb.setUpdateTime(new Date().toInstant());
            rdbList.add(rdb);
            rdbList.add(rdb);
        }
        return rdbList;
    }

    private List<RdbDO> createRdbList() {
        List<RdbDO> rdbList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            RdbDO rdb = new RdbDO();
            rdb.setId(Long.valueOf(i + 1));
            rdb.setRdbType("mysql");
            rdb.setName("a cdc-cluster-" + i);
            rdb.setUsername("root");
            rdb.setPassword("123456");
            rdb.setDescription("desc");
            rdb.setCreationTime(new Date().toInstant());
            rdb.setUpdateTime(new Date().toInstant());
            rdbList.add(rdb);
        }
        return rdbList;
    }
}
