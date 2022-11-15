package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
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

import javax.validation.ConstraintViolationException;
import java.util.Date;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class RdbDataBaseServiceIT {

    @Autowired
    private RdbDatabaseService rdbDatabaseService;

    @Autowired
    private RdbService rdbService;

    @Test
    public void testSaveShouldInsertWhenNotExist() {
        RdbDatabaseDO rdbDatabase = new RdbDatabaseDO();
        rdbDatabase.setName("rdbDatabase-test");
        rdbDatabase.setCreationTime(new Date().toInstant());
        rdbDatabase.setUpdateTime(new Date().toInstant());
        RdbDatabaseDO saveResult = rdbDatabaseService.save(rdbDatabase);
        rdbDatabase.setId(saveResult.getId());
        Assertions.assertThat(saveResult).isEqualTo(rdbDatabase);
    }

    @Test
    public void testSaveShouldUpdateWhenExist() {
        RdbDatabaseDO rdbDatabase = new RdbDatabaseDO();
        rdbDatabase.setName("rdbDatabase-test");
        rdbDatabase.setCreationTime(new Date().toInstant());
        rdbDatabase.setUpdateTime(new Date().toInstant());
        RdbDatabaseDO saveResult1 = rdbDatabaseService.save(rdbDatabase);
        saveResult1.setName("test2");
        RdbDatabaseDO saveResult2 = rdbDatabaseService.save(saveResult1);
        Assertions.assertThat(saveResult2.getName()).isEqualTo("test2");
        Assertions.assertThat(rdbDatabaseService.queryAll(new RdbDatabaseDO()).size()).isEqualTo(1);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testSaveShouldFailWhenMissingNotNullField() {
        RdbDatabaseDO rdbDatabase = new RdbDatabaseDO();
        rdbDatabase.setName(null);
        rdbDatabaseService.save(rdbDatabase);
    }

    @Test(expected = InvalidDataAccessApiUsageException.class)
    public void testSaveShouldFailWhenGivenNull() {
        rdbDatabaseService.save(null);
    }

    @Test
    public void testSaveAllShouldInsertWhenNotExist() {
        List<RdbDatabaseDO> rdbDatabaseList = createRdbDataBaseList();
        List<RdbDatabaseDO> saveResultList = rdbDatabaseService.saveAll(rdbDatabaseList);

        Assertions.assertThat(saveResultList.size()).isEqualTo(rdbDatabaseList.size());

        for (int i = 0; i < rdbDatabaseList.size(); i++) {
            rdbDatabaseList.get(i).setId(saveResultList.get(i).getId());
            Assertions.assertThat(saveResultList.get(i)).isEqualTo(rdbDatabaseList.get(i));
        }
    }

    @Test
    public void testSaveAllShouldUpdateWhenExist() {
        List<RdbDatabaseDO> rdbDatabaseList = createRdbDataBaseList();
        List<RdbDatabaseDO> saveResultList = rdbDatabaseService.saveAll(rdbDatabaseList);
        saveResultList.forEach(db -> db.setName("test_update"));
        rdbDatabaseService.saveAll(saveResultList).forEach(db -> {
            Assertions.assertThat(db.getName()).isEqualTo("test_update");
        });
        Assertions.assertThat(rdbDatabaseService.saveAll(saveResultList).size()).isEqualTo(rdbDatabaseList.size());
    }

    @Test(expected = ConstraintViolationException.class)
    public void testSaveAllShouldFailWhenMissingNotNullField() {
        List<RdbDatabaseDO> rdbDatabaseList = createRdbDataBaseList();
        rdbDatabaseList.forEach(rdb -> rdb.setName(null));
        rdbDatabaseService.saveAll(rdbDatabaseList);
    }

    @Test(expected = InvalidDataAccessApiUsageException.class)
    public void testSaveAllShouldFailWhenGivenNull() {
        rdbDatabaseService.saveAll(null);
    }

    @Test
    public void testQueryAllShouldSuccess() {
        List<RdbDatabaseDO> rdbDatabaseList = createRdbDatabaseListByCount(20);
        rdbDatabaseService.saveAll(rdbDatabaseList);
        RdbDatabaseDO rdbDatabase = new RdbDatabaseDO();
        rdbDatabase.setName("rdbDatabase1");
        List<RdbDatabaseDO> queryResultList = rdbDatabaseService.queryAll(rdbDatabase);
        Assertions.assertThat(queryResultList.size()).isEqualTo(10);
        queryResultList.forEach(r -> Assertions.assertThat(r.getName()).contains("rdbDatabase1"));

        queryResultList = rdbDatabaseService.queryAll(new RdbDatabaseDO());
        long p1Count = queryResultList.stream().filter(item -> item.getName().contains("rdbDatabase1")).count();
        long p2Count = queryResultList.stream().filter(item -> item.getName().contains("rdbDatabase2")).count();
        Assertions.assertThat(queryResultList.size()).isEqualTo(20);
        Assertions.assertThat(p1Count).isEqualTo(10);
        Assertions.assertThat(p2Count).isEqualTo(10);
    }

    @Test(expected = NullPointerException.class)
    public void testQueryAllShouldFailWhenGiveNull() {
        rdbDatabaseService.queryAll(null);
    }

    @Test
    public void testQuery() {
        List<RdbDatabaseDO> rdbDatabaseList = createRdbDataBaseList();
        rdbDatabaseService.saveAll(rdbDatabaseList);

        // 分页正常滚动
        RdbDatabaseDO rdbDatabase = new RdbDatabaseDO();
        rdbDatabase.setName("rdbDatabase");
        Page<RdbDatabaseDO> page = rdbDatabaseService.query(rdbDatabase, createPageRequest(1, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = rdbDatabaseService.query(rdbDatabase, createPageRequest(2, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = rdbDatabaseService.query(rdbDatabase, createPageRequest(3, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = rdbDatabaseService.query(rdbDatabase, createPageRequest(4, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(1);

        // 过滤条件不存在
        rdbDatabase.setName("kk-not-exist");
        page = rdbDatabaseService.query(rdbDatabase, createPageRequest(1, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(0);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(0);
        Assertions.assertThat(page.getContent().size()).isEqualTo(0);

        // 更改pageSize,取消传入查询条件
        page = rdbDatabaseService.query(new RdbDatabaseDO(), createPageRequest(1, 10));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(1);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(10);

        // 页越界
        rdbDatabase.setName("rdbDatabase");
        page = rdbDatabaseService.query(rdbDatabase, createPageRequest(999, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueryShouldFailWhenGivenIllegalPageIndex() {
        rdbDatabaseService.query(new RdbDatabaseDO(), createPageRequest(-999, -666));
    }

    @Test
    public void testCascade() {
        RdbDO rdb = new RdbDO();
        rdb.setId(1L);
        rdb.setRdbType("mysql");
        rdb.setName("a cdc-cluster");
        rdb.setUsername("root");
        rdb.setPassword("123456");
        rdb.setDescription("desc");
        RdbDO rdbSaveResult = rdbService.save(rdb);

        RdbDatabaseDO rdbDatabase = new RdbDatabaseDO();
        rdbDatabase.setName("rdbDatabase-test");
        rdbDatabase.setCreationTime(new Date().toInstant());
        rdbDatabase.setUpdateTime(new Date().toInstant());
        rdbDatabase.setRdb(rdb);
        RdbDatabaseDO rdbDtaBaseSaveResult = rdbDatabaseService.save(rdbDatabase);
        Assertions.assertThat(rdbDtaBaseSaveResult.getRdb()).isEqualTo(rdbSaveResult);
    }

    private Pageable createPageRequest(final int pageIndex, final int pageSize) {
        Pageable pageable = PageRequest.of(pageIndex - 1, pageSize, Sort.by(Sort.Order.desc("name")));
        return pageable;
    }

    private List<RdbDatabaseDO> createRdbDatabaseListByCount(int count) {
        Assert.assertTrue(count >= 1);

        List<RdbDatabaseDO> rdbDatabaseList = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            String name = i % 2 == 0 ? "rdbDatabase1-" + i : "rdbDatabase2-" + i;
            RdbDatabaseDO rdbDatabase = new RdbDatabaseDO();
            rdbDatabase.setName(name);
            rdbDatabase.setCreationTime(new Date().toInstant());
            rdbDatabase.setUpdateTime(new Date().toInstant());
            rdbDatabaseList.add(rdbDatabase);
        }
        return rdbDatabaseList;
    }

    private List<RdbDatabaseDO> createRdbDataBaseList() {
        List<RdbDatabaseDO> rdbDatabaseList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            RdbDatabaseDO rdbDatabase = new RdbDatabaseDO();
            rdbDatabase.setName("rdbDatabase" + i);
            rdbDatabase.setCreationTime(new Date().toInstant());
            rdbDatabase.setUpdateTime(new Date().toInstant());
            rdbDatabaseList.add(rdbDatabase);
        }
        return rdbDatabaseList;
    }
}
