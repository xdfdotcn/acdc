package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
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
public class RdbTableServiceIT {

    @Autowired
    private RdbTableService rdbTableService;

    @Autowired
    private RdbDatabaseService rdbDatabaseService;

    @Test
    public void testSaveShouldInsertWhenNotExist() {
        RdbTableDO rdbTable = new RdbTableDO();
        rdbTable.setName("test");
        RdbTableDO rdbTableSaveResult = rdbTableService.save(rdbTable);
        Assertions.assertThat(rdbTableSaveResult).isEqualTo(rdbTable);
    }

    @Test
    public void testSaveShouldUpdateWhenExist() {
        RdbTableDO rdbTable = new RdbTableDO();
        rdbTable.setName("test");
        RdbTableDO saveResult1 = rdbTableService.save(rdbTable);
        saveResult1.setName("test2");
        RdbTableDO saveResult2 = rdbTableService.save(saveResult1);
        Assertions.assertThat(saveResult2.getName()).isEqualTo("test2");
        Assertions.assertThat(rdbTableService.queryAll(new RdbTableDO()).size()).isEqualTo(1);
    }

    @Test
    public void testSaveShouldThrowExceptionWhenFieldValidateFail() {
        RdbTableDO rdbTable = new RdbTableDO();
        rdbTable.setName(null);
        Throwable throwable = Assertions.catchThrowable(() -> rdbTableService.save(rdbTable));
        Assertions.assertThat(throwable).isInstanceOf(ConstraintViolationException.class);
    }

    @Test
    public void testSaveAllShouldInsertWhenNotExist() {
        List<RdbTableDO> rdbTableList = createRdbTableList();
        List<RdbTableDO> saveResultList = rdbTableService.saveAll(rdbTableList);

        Assertions.assertThat(saveResultList.size()).isEqualTo(rdbTableList.size());

        for (int i = 0; i < rdbTableList.size(); i++) {
            rdbTableList.get(i).setId(saveResultList.get(i).getId());
            Assertions.assertThat(saveResultList.get(i)).isEqualTo(rdbTableList.get(i));
        }
    }

    @Test
    public void testSaveAllShouldUpdateWhenAlreadyExist() {
        List<RdbTableDO> rdbTableList = createRdbTableList();
        List<RdbTableDO> saveResultList = rdbTableService.saveAll(rdbTableList);
        saveResultList.forEach(table -> table.setName("test_update"));
        rdbTableService.saveAll(saveResultList).forEach(table -> {
            Assertions.assertThat(table.getName()).isEqualTo("test_update");
        });
        Assertions.assertThat(rdbTableService.saveAll(saveResultList).size()).isEqualTo(rdbTableList.size());
    }

    @Test
    public void testSaveAllShouldDoNothingWhenGivenEmptyCollection() {
        rdbTableService.saveAll(Lists.newArrayList());
        Assertions.assertThat(rdbTableService.queryAll(new RdbTableDO()).size()).isEqualTo(0);
    }

    @Test
    public void testSaveAllShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable = Assertions.catchThrowable(() -> rdbTableService.saveAll(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }

    @Test
    public void testQueryAllShouldSuccess() {
        List<RdbTableDO> rdbTableList = createRdbTableListByCount(20);
        rdbTableService.saveAll(rdbTableList);
        RdbTableDO rdbTable = new RdbTableDO();
        rdbTable.setName("rdbTable1");
        List<RdbTableDO> queryResultList = rdbTableService.queryAll(rdbTable);
        Assertions.assertThat(queryResultList.size()).isEqualTo(10);
        queryResultList.forEach(r -> Assertions.assertThat(r.getName()).contains("rdbTable1"));

        queryResultList = rdbTableService.queryAll(new RdbTableDO());
        long p1Count = queryResultList.stream().filter(item -> item.getName().contains("rdbTable1")).count();
        long p2Count = queryResultList.stream().filter(item -> item.getName().contains("rdbTable2")).count();
        Assertions.assertThat(queryResultList.size()).isEqualTo(20);
        Assertions.assertThat(p1Count).isEqualTo(10);
        Assertions.assertThat(p2Count).isEqualTo(10);
    }

    @Test
    public void testQueryAllShouldGetEmpty() {
        List<RdbTableDO> rdbTables = rdbTableService.queryAll(new RdbTableDO());
        Assertions.assertThat(rdbTables.isEmpty()).isEqualTo(true);
    }

    @Test
    public void testQueryAllShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable = Assertions.catchThrowable(() -> rdbTableService.queryAll(null));
        Assertions.assertThat(throwable).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testQueryShouldSuccess() {
        List<RdbTableDO> rdbTableList = createRdbTableList();
        rdbTableService.saveAll(rdbTableList);

        // 分页正常滚动
        RdbTableDO rdbTable = new RdbTableDO();
        rdbTable.setName("rdbTable");
        Page<RdbTableDO> page = rdbTableService.query(rdbTable, createPageRequest(1, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = rdbTableService.query(rdbTable, createPageRequest(2, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = rdbTableService.query(rdbTable, createPageRequest(3, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = rdbTableService.query(rdbTable, createPageRequest(4, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(1);

        // 过滤条件不存在
        rdbTable.setName("kk-not-exist");
        page = rdbTableService.query(rdbTable, createPageRequest(1, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(0);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(0);
        Assertions.assertThat(page.getContent().size()).isEqualTo(0);

        // 更改pageSize,取消传入查询条件
        page = rdbTableService.query(new RdbTableDO(), createPageRequest(1, 10));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(1);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(10);

        // 页越界
        rdbTable.setName("rdbTable");
        page = rdbTableService.query(rdbTable, createPageRequest(999, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(0);
    }

    @Test
    public void testQueryShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable = Assertions.catchThrowable(() -> rdbTableService.query(null, null));
        Assertions.assertThat(throwable).isInstanceOf(NullPointerException.class);

        throwable = Assertions.catchThrowable(() -> rdbTableService.query(new RdbTableDO(), null));
        Assertions.assertThat(throwable).isInstanceOf(NullPointerException.class);

        throwable = Assertions.catchThrowable(() -> rdbTableService.query(new RdbTableDO(), createPageRequest(-999, -666)));
        Assertions.assertThat(throwable).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testFindByIdShouldSuccess() {
        RdbTableDO rdbTable = new RdbTableDO();
        rdbTable.setName("test");
        RdbTableDO rdbTableSaveResult = rdbTableService.save(rdbTable);
        Assertions.assertThat(rdbTableService.findById(rdbTableSaveResult.getId()).isPresent()).isEqualTo(true);
        Assertions.assertThat(rdbTableService.findById(99L).isPresent()).isEqualTo(false);
    }

    @Test
    public void testFindByIdShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable = Assertions.catchThrowable(() -> rdbTableService.findById(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }

    @Test
    public void testCascade() {

        RdbDatabaseDO rdbDatabase = new RdbDatabaseDO();
        rdbDatabase.setName("rdbDatabase-test");
        rdbDatabase.setCreationTime(new Date().toInstant());
        rdbDatabase.setUpdateTime(new Date().toInstant());
        RdbDatabaseDO rdbSaveResult = rdbDatabaseService.save(rdbDatabase);

        RdbTableDO rdbTable = new RdbTableDO();
        rdbTable.setName("test");
        rdbTable.setRdbDatabase(rdbSaveResult);
        RdbTableDO rdbTableSaveResult = rdbTableService.save(rdbTable);

        Assertions.assertThat(rdbTableService.findById(rdbTableSaveResult.getId()).get().getRdbDatabase())
                .isEqualTo(rdbSaveResult);
    }

    private Pageable createPageRequest(final int pageIndex, final int pageSize) {
        Pageable pageable = PageRequest.of(pageIndex - 1, pageSize, Sort.by(Sort.Order.desc("name")));
        return pageable;
    }

    private List<RdbTableDO> createRdbTableListByCount(int count) {
        Assert.assertTrue(count >= 1);

        List<RdbTableDO> rdbTableList = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            String name = i % 2 == 0 ? "rdbTable1-" + i : "rdbTable2-" + i;
            RdbTableDO rdbTable = new RdbTableDO();
            rdbTable.setName(name);
            rdbTableList.add(rdbTable);
        }
        return rdbTableList;
    }

    private List<RdbTableDO> createRdbTableList() {
        List<RdbTableDO> rdbTableList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            RdbTableDO rdbTable = new RdbTableDO();
            rdbTable.setName("rdbTable" + i);
            rdbTableList.add(rdbTable);
        }
        return rdbTableList;
    }
}
